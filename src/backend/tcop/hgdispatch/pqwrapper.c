
#include "postgres.h"
#include "pqwrapper.h"
#include "libpq/libpq-be.h"
#include "poll.h"

extern struct Port* MyProcPort;

int	hgSendSome(PGconn *conn, int len);
int	hgSocketCheck(PGconn *conn, int forRead, int forWrite,
					  time_t end_time);
int	hgSocketPoll(int sock, int forRead, int forWrite, time_t end_time);


/*
 *	Read data from a secure connection.
 *
 * On failure, this function is responsible for putting a suitable message
 * into conn->errorMessage.  The caller must still inspect errno, but only
 * to determine whether to continue/retry after error.
 */
ssize_t
hgsecure_read(PGconn *conn, void *ptr, size_t len)
{
	ssize_t		n;

	n = hgsecure_raw_read(conn, ptr, len);

	return n;
}

ssize_t
hgsecure_raw_read(PGconn *conn, void *ptr, size_t len)
{
	ssize_t		n;
	int			result_errno = 0;
	char		sebuf[PG_STRERROR_R_BUFLEN];

	n = recv(conn->sock, ptr, len, 0);

	if (n < 0)
	{
		result_errno = SOCK_ERRNO;

		/* Set error message if appropriate */
		switch (result_errno)
		{
#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				/* no error message, caller is expected to retry */
				break;

#ifdef ECONNRESET
			case ECONNRESET:
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("server closed the connection unexpectedly\n"
												"\tThis probably means the server terminated abnormally\n"
												"\tbefore or while processing the request.\n"));
				break;
#endif

			default:
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("could not receive data from server: %s\n"),
								  SOCK_STRERROR(result_errno,
												sebuf, sizeof(sebuf)));
				break;
		}
	}

	/* ensure we return the intended errno to caller */
	SOCK_ERRNO_SET(result_errno);

	return n;
}

ssize_t
hgsecure_raw_write(Port *port, const void *ptr, size_t len)
{
	ssize_t		n;
	int			flags = 0;
	int			result_errno = 0;
	char		sebuf[PG_STRERROR_R_BUFLEN];

	n = send(port->sock, ptr, len, flags);

	if (n < 0)
	{
		result_errno = SOCK_ERRNO;
		ereport(LOG,
				(errmsg("fail to send mesage due to %s",
						SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)))));
	}


	return n;
}



/*
 * pqGetc: get 1 character from the connection
 *
 *	All these routines return 0 on success, EOF on error.
 *	Note that for the Get routines, EOF only means there is not enough
 *	data in the buffer, not that there is necessarily a hard error.
 */
int
hgGetc(char *result, PGconn *conn)
{
	if (conn->inCursor >= conn->inEnd)
		return EOF;

	*result = conn->inBuffer[conn->inCursor++];

	if (conn->Pfdebug)
		fprintf(conn->Pfdebug, "From backend> %c\n", *result);

	return 0;
}


/*
 * pqGets[_append]:
 * get a null-terminated string from the connection,
 * and store it in an expansible PQExpBuffer.
 * If we run out of memory, all of the string is still read,
 * but the excess characters are silently discarded.
 */
static int
hgGets_internal(PQExpBuffer buf, PGconn *conn, bool resetbuffer)
{
	/* Copy conn data to locals for faster search loop */
	char	   *inBuffer = conn->inBuffer;
	int			inCursor = conn->inCursor;
	int			inEnd = conn->inEnd;
	int			slen;

	while (inCursor < inEnd && inBuffer[inCursor])
		inCursor++;

	if (inCursor >= inEnd)
		return EOF;

	slen = inCursor - conn->inCursor;

	if (resetbuffer)
		resetPQExpBuffer(buf);

	appendBinaryPQExpBuffer(buf, inBuffer + conn->inCursor, slen);

	conn->inCursor = ++inCursor;

	if (conn->Pfdebug)
		fprintf(conn->Pfdebug, "From backend> \"%s\"\n",
				buf->data);

	return 0;
}

int
hgGets(PQExpBuffer buf, PGconn *conn)
{
	return hgGets_internal(buf, conn, true);
}

int
hgGets_append(PQExpBuffer buf, PGconn *conn)
{
	return hgGets_internal(buf, conn, false);
}


/*
 * pqGetInt
 *	read a 2 or 4 byte integer and convert from network byte order
 *	to local byte order
 */
int
hgGetInt(int *result, size_t bytes, PGconn *conn)
{
	uint16		tmp2;
	uint32		tmp4;

	switch (bytes)
	{
		case 2:
			if (conn->inCursor + 2 > conn->inEnd)
				return EOF;
			memcpy(&tmp2, conn->inBuffer + conn->inCursor, 2);
			conn->inCursor += 2;
			*result = (int) pg_ntoh16(tmp2);
			break;
		case 4:
			if (conn->inCursor + 4 > conn->inEnd)
				return EOF;
			memcpy(&tmp4, conn->inBuffer + conn->inCursor, 4);
			conn->inCursor += 4;
			*result = (int) pg_ntoh32(tmp4);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("integer of size %lu not supported by pqGetInt")));
			return EOF;
	}

	return 0;
}

/*
 * Make sure conn's output buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int
hgCheckOutBufferSpace(size_t bytes_needed, PGconn *conn)
{
	int			newsize = conn->outBufSize;
	char	   *newbuf;

	/* Quick exit if we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * If we need to enlarge the buffer, we first try to double it in size; if
	 * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
	 * the malloc pool by repeated small enlargements.
	 *
	 * Note: tests for newsize > 0 are to catch integer overflow.
	 */
	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->outBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->outBuffer = newbuf;
			conn->outBufSize = newsize;
			return 0;
		}
	}

	newsize = conn->outBufSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->outBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->outBuffer = newbuf;
			conn->outBufSize = newsize;
			return 0;
		}
	}

	/* realloc failed. Probably out of memory */
	printfPQExpBuffer(&conn->errorMessage,
					  "cannot allocate memory for output buffer\n");
	return EOF;
}

/*
 * Make sure conn's input buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int
hgCheckInBufferSpace(size_t bytes_needed, PGconn *conn)
{
	int			newsize = conn->inBufSize;
	char	   *newbuf;

	/* Quick exit if we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * Before concluding that we need to enlarge the buffer, left-justify
	 * whatever is in it and recheck.  The caller's value of bytes_needed
	 * includes any data to the left of inStart, but we can delete that in
	 * preference to enlarging the buffer.  It's slightly ugly to have this
	 * function do this, but it's better than making callers worry about it.
	 */
	bytes_needed -= conn->inStart;

	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/* Recheck whether we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * If we need to enlarge the buffer, we first try to double it in size; if
	 * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
	 * the malloc pool by repeated small enlargements.
	 *
	 * Note: tests for newsize > 0 are to catch integer overflow.
	 */
	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->inBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->inBuffer = newbuf;
			conn->inBufSize = newsize;
			return 0;
		}
	}

	newsize = conn->inBufSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->inBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->inBuffer = newbuf;
			conn->inBufSize = newsize;
			return 0;
		}
	}

	/* realloc failed. Probably out of memory */
	printfPQExpBuffer(&conn->errorMessage,
					  "cannot allocate memory for input buffer\n");
	return EOF;
}


/* ----------
 * pqReadData: read more data, if any is available
 * Possible return values:
 *	 1: successfully loaded at least one more byte
 *	 0: no data is presently available, but no error detected
 *	-1: error detected (including EOF = connection closure);
 *		conn->errorMessage set
 * NOTE: callers must not assume that pointers or indexes into conn->inBuffer
 * remain valid across this call!
 * ----------
 */
int
hgReadData(PGconn *conn)
{
	int			someread = 0;
	int			nread;

	if (conn->sock == PGINVALID_SOCKET)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("connection not open\n"));
		return -1;
	}

	/* Left-justify any data in the buffer to make room */
	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/*
	 * If the buffer is fairly full, enlarge it. We need to be able to enlarge
	 * the buffer in case a single message exceeds the initial buffer size. We
	 * enlarge before filling the buffer entirely so as to avoid asking the
	 * kernel for a partial packet. The magic constant here should be large
	 * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
	 * buffer size, so...
	 */
	if (conn->inBufSize - conn->inEnd < 8192)
	{
		if (hgCheckInBufferSpace(conn->inEnd + (size_t) 8192, conn))
		{
			/*
			 * We don't insist that the enlarge worked, but we need some room
			 */
			if (conn->inBufSize - conn->inEnd < 100)
				return -1;		/* errorMessage already set */
		}
	}

	/* OK, try to read some data */
retry3:
	nread = hgsecure_read(conn, conn->inBuffer + conn->inEnd,
						  conn->inBufSize - conn->inEnd);
	if (nread < 0)
	{
		if (SOCK_ERRNO == EINTR)
			goto retry3;
		/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
		if (SOCK_ERRNO == EAGAIN)
			return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
		if (SOCK_ERRNO == EWOULDBLOCK)
			return someread;
#endif
		/* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
		if (SOCK_ERRNO == ECONNRESET)
			goto definitelyFailed;
#endif
		/* pqsecure_read set the error message for us */
		return -1;
	}
	if (nread > 0)
	{
		conn->inEnd += nread;

		/*
		 * Hack to deal with the fact that some kernels will only give us back
		 * 1 packet per recv() call, even if we asked for more and there is
		 * more available.  If it looks like we are reading a long message,
		 * loop back to recv() again immediately, until we run out of data or
		 * buffer space.  Without this, the block-and-restart behavior of
		 * libpq's higher levels leads to O(N^2) performance on long messages.
		 *
		 * Since we left-justified the data above, conn->inEnd gives the
		 * amount of data already read in the current message.  We consider
		 * the message "long" once we have acquired 32k ...
		 */
		if (conn->inEnd > 32768 &&
			(conn->inBufSize - conn->inEnd) >= 8192)
		{
			someread = 1;
			goto retry3;
		}
		return 1;
	}

	if (someread)
		return 1;				/* got a zero read after successful tries */

	/*
	 * A return value of 0 could mean just that no data is now available, or
	 * it could mean EOF --- that is, the server has closed the connection.
	 * Since we have the socket in nonblock mode, the only way to tell the
	 * difference is to see if select() is saying that the file is ready.
	 * Grumble.  Fortunately, we don't expect this path to be taken much,
	 * since in normal practice we should not be trying to read data unless
	 * the file selected for reading already.
	 *
	 * In SSL mode it's even worse: SSL_read() could say WANT_READ and then
	 * data could arrive before we make the pqReadReady() test, but the second
	 * SSL_read() could still say WANT_READ because the data received was not
	 * a complete SSL record.  So we must play dumb and assume there is more
	 * data, relying on the SSL layer to detect true EOF.
	 */

#ifdef USE_SSL
	if (conn->ssl_in_use)
		return 0;
#endif

	switch (hgReadReady(conn))
	{
	case 0:
		/* definitely no data available */
		return 0;
	case 1:
		/* ready for read */
		break;
	default:
		/* we override pqReadReady's message with something more useful */
		goto definitelyEOF;
	}

	/*
	 * Still not sure that it's EOF, because some data could have just
	 * arrived.
	 */
retry4:
	nread = hgsecure_read(conn, conn->inBuffer + conn->inEnd,
						  conn->inBufSize - conn->inEnd);
	if (nread < 0)
	{
		if (SOCK_ERRNO == EINTR)
			goto retry4;
		/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
		if (SOCK_ERRNO == EAGAIN)
			return 0;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
		if (SOCK_ERRNO == EWOULDBLOCK)
			return 0;
#endif
		/* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
		if (SOCK_ERRNO == ECONNRESET)
			goto definitelyFailed;
#endif
		/* pqsecure_read set the error message for us */
		return -1;
	}
	if (nread > 0)
	{
		conn->inEnd += nread;
		return 1;
	}

	/*
	 * OK, we are getting a zero read even though select() says ready. This
	 * means the connection has been closed.  Cope.
	 */
definitelyEOF:
	printfPQExpBuffer(&conn->errorMessage,
					  libpq_gettext("server closed the connection unexpectedly\n"
									"\tThis probably means the server terminated abnormally\n"
									"\tbefore or while processing the request.\n"));

	/* Come here if lower-level code already set a suitable errorMessage */
definitelyFailed:
	/* Do *not* drop any already-read data; caller still wants it */
	hgDropConnection(conn, false);
	conn->status = CONNECTION_BAD;	/* No more connection to backend */
	return -1;
}

/*
 * pqSendSome: send data waiting in the output buffer.
 *
 * len is how much to try to send (typically equal to outCount, but may
 * be less).
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 *
 * Note that this is also responsible for consuming data from the socket
 * (putting it in conn->inBuffer) in any situation where we can't send
 * all the specified data immediately.
 *
 * Upon write failure, conn->write_failed is set and the error message is
 * saved in conn->write_err_msg, but we clear the output buffer and return
 * zero anyway; this is because callers should soldier on until it's possible
 * to read from the server and check for an error message.  write_err_msg
 * should be reported only when we are unable to obtain a server error first.
 * (Thus, a -1 result is returned only for an internal *read* failure.)
 */
int
hgSendSome(PGconn *conn, int len)
{
	char	   *ptr = conn->outBuffer;
	int			remaining = conn->outCount;
	int			result = 0;

	/*
	 * If we already had a write failure, we will never again try to send data
	 * on that connection.  Even if the kernel would let us, we've probably
	 * lost message boundary sync with the server.  conn->write_failed
	 * therefore persists until the connection is reset, and we just discard
	 * all data presented to be written.  However, as long as we still have a
	 * valid socket, we should continue to absorb data from the backend, so
	 * that we can collect any final error messages.
	 */
	if (conn->write_failed)
	{
		/* conn->write_err_msg should be set up already */
		conn->outCount = 0;
		/* Absorb input data if any, and detect socket closure */
		if (conn->sock != PGINVALID_SOCKET)
		{
			if (hgReadData(conn) < 0)
				return -1;
		}
		return 0;
	}

	if (conn->sock == PGINVALID_SOCKET)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("connection not open\n"));
		conn->write_failed = true;
		/* Transfer error message to conn->write_err_msg, if possible */
		/* (strdup failure is OK, we'll cope later) */
		conn->write_err_msg = strdup(conn->errorMessage.data);
		resetPQExpBuffer(&conn->errorMessage);
		/* Discard queued data; no chance it'll ever be sent */
		conn->outCount = 0;
		return 0;
	}

	/* while there's still data to send */
	while (len > 0)
	{
		int			sent;
		int result_errno = 0;
		char		sebuf[PG_STRERROR_R_BUFLEN];
		
		sent = send(conn->sock, ptr, len, 0);

		if (sent < 0)
		{
			result_errno = SOCK_ERRNO;
			ereport(LOG,
				(errmsg("fail to send mesage due to %s",
						SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)))));
			/* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
			switch (SOCK_ERRNO)
			{
#ifdef EAGAIN
			case EAGAIN:
				break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
				break;
#endif
			case EINTR:
				continue;

			default:
				/* pqsecure_write set the error message for us */
				conn->write_failed = true;

				/*
				 * Transfer error message to conn->write_err_msg, if
				 * possible (strdup failure is OK, we'll cope later).
				 *
				 * Note: this assumes that pqsecure_write and its children
				 * will overwrite not append to conn->errorMessage.  If
				 * that's ever changed, we could remember the length of
				 * conn->errorMessage at entry to this routine, and then
				 * save and delete just what was appended.
				 */
				conn->write_err_msg = strdup(conn->errorMessage.data);
				resetPQExpBuffer(&conn->errorMessage);

				/* Discard queued data; no chance it'll ever be sent */
				conn->outCount = 0;

				/* Absorb input data if any, and detect socket closure */
				if (conn->sock != PGINVALID_SOCKET)
				{
					if (hgReadData(conn) < 0)
						return -1;
				}
				return 0;
			}
		}
		else
		{
			ptr += sent;
			len -= sent;
			remaining -= sent;
		}

		if (len > 0)
		{
			/*
			 * We didn't send it all, wait till we can send more.
			 *
			 * There are scenarios in which we can't send data because the
			 * communications channel is full, but we cannot expect the server
			 * to clear the channel eventually because it's blocked trying to
			 * send data to us.  (This can happen when we are sending a large
			 * amount of COPY data, and the server has generated lots of
			 * NOTICE responses.)  To avoid a deadlock situation, we must be
			 * prepared to accept and buffer incoming data before we try
			 * again.  Furthermore, it is possible that such incoming data
			 * might not arrive until after we've gone to sleep.  Therefore,
			 * we wait for either read ready or write ready.
			 *
			 * In non-blocking mode, we don't wait here directly, but return 1
			 * to indicate that data is still pending.  The caller should wait
			 * for both read and write ready conditions, and call
			 * PQconsumeInput() on read ready, but just in case it doesn't, we
			 * call pqReadData() ourselves before returning.  That's not
			 * enough if the data has not arrived yet, but it's the best we
			 * can do, and works pretty well in practice.  (The documentation
			 * used to say that you only need to wait for write-ready, so
			 * there are still plenty of applications like that out there.)
			 *
			 * Note that errors here don't result in write_failed becoming
			 * set.
			 */
			if (hgReadData(conn) < 0)
			{
				result = -1;	/* error message already set up */
				break;
			}

			if (PQisnonblocking(conn))
			{
				result = 1;
				break;
			}

			if (hgWait(true, true, conn))
			{
				result = -1;
				break;
			}
		}
	}

	/* shift the remaining contents of the buffer */
	if (remaining > 0)
		memmove(conn->outBuffer, ptr, remaining);
	conn->outCount = remaining;

	return result;
}


/*
 * pqFlush: send any data waiting in the output buffer
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 * (See pqSendSome comments about how failure should be handled.)
 */
int
hgFlush(PGconn *conn)
{
	if (conn->Pfdebug)
		fflush(conn->Pfdebug);

	if (conn->outCount > 0)
		return hgSendSome(conn, conn->outCount);

	return 0;
}


/*
 * pqWait: wait until we can read or write the connection socket
 *
 * JAB: If SSL enabled and used and forRead, buffered bytes short-circuit the
 * call to select().
 *
 * We also stop waiting and return if the kernel flags an exception condition
 * on the socket.  The actual error condition will be detected and reported
 * when the caller tries to read or write the socket.
 */
int
hgWait(int forRead, int forWrite, PGconn *conn)
{
	return hgWaitTimed(forRead, forWrite, conn, (time_t) -1);
}

/*
 * pqWaitTimed: wait, but not past finish_time.
 *
 * finish_time = ((time_t) -1) disables the wait limit.
 *
 * Returns -1 on failure, 0 if the socket is readable/writable, 1 if it timed out.
 */
int
hgWaitTimed(int forRead, int forWrite, PGconn *conn, time_t finish_time)
{
	int			result;

	result = hgSocketCheck(conn, forRead, forWrite, finish_time);

	if (result < 0)
		return -1;				/* errorMessage is already set */

	if (result == 0)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("timeout expired\n"));
		return 1;
	}

	return 0;
}

/*
 * pqReadReady: is select() saying the file is ready to read?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int
hgReadReady(PGconn *conn)
{
	return hgSocketCheck(conn, 1, 0, (time_t) 0);
}

/*
 * pqWriteReady: is select() saying the file is ready to write?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int
hgWriteReady(PGconn *conn)
{
	return hgSocketCheck(conn, 0, 1, (time_t) 0);
}

/*
 * Checks a socket, using poll or select, for data to be read, written,
 * or both.  Returns >0 if one or more conditions are met, 0 if it timed
 * out, -1 if an error occurred.
 *
 * If SSL is in use, the SSL buffer is checked prior to checking the socket
 * for read data directly.
 */
int
hgSocketCheck(PGconn *conn, int forRead, int forWrite, time_t end_time)
{
	int			result;

	if (!conn)
		return -1;
	if (conn->sock == PGINVALID_SOCKET)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("invalid socket\n"));
		return -1;
	}

#ifdef USE_SSL
	/* Check for SSL library buffering read bytes */
	if (forRead && conn->ssl_in_use && pgtls_read_pending(conn))
	{
		/* short-circuit the select */
		return 1;
	}
#endif

	/* We will retry as long as we get EINTR */
	do
		result = hgSocketPoll(conn->sock, forRead, forWrite, end_time);
	while (result < 0 && SOCK_ERRNO == EINTR);

	if (result < 0)
	{
		char		sebuf[PG_STRERROR_R_BUFLEN];

		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("select() failed: %s\n"),
						  SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
	}

	return result;
}


/*
 * Check a file descriptor for read and/or write data, possibly waiting.
 * If neither forRead nor forWrite are set, immediately return a timeout
 * condition (without waiting).  Return >0 if condition is met, 0
 * if a timeout occurred, -1 if an error or interrupt occurred.
 *
 * Timeout is infinite if end_time is -1.  Timeout is immediate (no blocking)
 * if end_time is 0 (or indeed, any time before now).
 */
int
hgSocketPoll(int sock, int forRead, int forWrite, time_t end_time)
{
	/* We use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
	struct pollfd input_fd;
	int			timeout_ms;

	if (!forRead && !forWrite)
		return 0;

	input_fd.fd = sock;
	input_fd.events = POLLERR;
	input_fd.revents = 0;

	if (forRead)
		input_fd.events |= POLLIN;
	if (forWrite)
		input_fd.events |= POLLOUT;

	/* Compute appropriate timeout interval */
	if (end_time == ((time_t) -1))
		timeout_ms = -1;
	else
	{
		time_t		now = time(NULL);

		if (end_time > now)
			timeout_ms = (end_time - now) * 1000;
		else
			timeout_ms = 0;
	}

	return poll(&input_fd, 1, timeout_ms);
#else							/* !HAVE_POLL */

	fd_set		input_mask;
	fd_set		output_mask;
	fd_set		except_mask;
	struct timeval timeout;
	struct timeval *ptr_timeout;

	if (!forRead && !forWrite)
		return 0;

	FD_ZERO(&input_mask);
	FD_ZERO(&output_mask);
	FD_ZERO(&except_mask);
	if (forRead)
		FD_SET(sock, &input_mask);

	if (forWrite)
		FD_SET(sock, &output_mask);
	FD_SET(sock, &except_mask);

	/* Compute appropriate timeout interval */
	if (end_time == ((time_t) -1))
		ptr_timeout = NULL;
	else
	{
		time_t		now = time(NULL);

		if (end_time > now)
			timeout.tv_sec = end_time - now;
		else
			timeout.tv_sec = 0;
		timeout.tv_usec = 0;
		ptr_timeout = &timeout;
	}

	return select(sock + 1, &input_mask, &output_mask,
				  &except_mask, ptr_timeout);
#endif							/* HAVE_POLL */
}


#ifdef ENABLE_NLS

static void
libpq_binddomain(void)
{
	static bool already_bound = false;

	if (!already_bound)
	{
		/* bindtextdomain() does not preserve errno */
		int			save_errno = errno;

		const char *ldir;

		already_bound = true;
		/* No relocatable lookup here because the binary could be anywhere */
		ldir = getenv("PGLOCALEDIR");
		if (!ldir)
			ldir = LOCALEDIR;
		bindtextdomain(PG_TEXTDOMAIN("libpq"), ldir);

		errno = save_errno;
	}
}

char *
libpq_gettext(const char *msgid)
{
	libpq_binddomain();
	return dgettext(PG_TEXTDOMAIN("libpq"), msgid);
}

char *
libpq_ngettext(const char *msgid, const char *msgid_plural, unsigned long n)
{
	libpq_binddomain();
	return dngettext(PG_TEXTDOMAIN("libpq"), msgid, msgid_plural, n);
}

#endif							/* ENABLE_NLS */

void hgDropConnection(PGconn *conn, bool flushInput)
{
	/* Drop any SSL state */
	/* Close the socket itself */
	if (conn->sock != PGINVALID_SOCKET)
		closesocket(conn->sock);
	conn->sock = PGINVALID_SOCKET;

	/* Optionally discard any unread data */
	if (flushInput)
		conn->inStart = conn->inCursor = conn->inEnd = 0;

	/* Always discard any unsent data */
	conn->outCount = 0;

	/* Free authentication/encryption state */
}

void hg_putbytes(char *buf, int len) {
	int n = hgsecure_raw_write(MyProcPort, buf, len);
	if (n < 0) {
		ereport(ERROR,
				errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				errmsg("could not send data !"));
	}
}


uint16 pg_ntoh16(uint16 x) {
	return
		((x << 8) & 0xff00) |
		((x >> 8) & 0x00ff);
}

uint32 pg_ntoh32(uint32 x) {
	return
		((x << 24) & 0xff000000) |
		((x << 8) & 0x00ff0000) |
		((x >> 8) & 0x0000ff00) |
		((x >> 24) & 0x000000ff);
}

uint32 pq_hton32(uint32 x) {
	{
		return
			((x << 24) & 0xff000000) |
			((x << 8) & 0x00ff0000) |
			((x >> 8) & 0x0000ff00) |
			((x >> 24) & 0x000000ff);
	}
}
