#include "hgdispatch.h"
#include "libpq-int.h"


//static DispatchPipline *Dispatch_Pipline = NULL;

#define BUFFER_LEN 4096
#define offsetof(type, field) ((unsigned long)&(((type *)0)->field))
#define IOBUF_SIZE (offsetof(IOBuf, buf) + BUFFER_LEN)
#define VALID_LONG_MESSAGE_TYPE(id) \
	((id) == 'T' || (id) == 'D' || (id) == 'd' || (id) == 'V' || \
	 (id) == 'E' || (id) == 'N' || (id) == 'A')


void dispatchInputParseAndSend(PGconn *conn);


DispatchState* createDispatchState() {
	DispatchState *st = palloc(sizeof(DispatchState));

	PGconn *c = getCurrentDispatchConnection();
	st->conn = c;
	return st;
}

bool dispatch(const char* query_string) {
	ereport(LOG, (errmsg("dispatch enter")));
	DispatchState *dstate = createDispatchState();
	ereport(LOG, (errmsg("going to do pgexec")));
	
	PGresult *res = PQsendQuery(dstate->conn, query_string);

	ereport(LOG, (errmsg("after pqexec")));
	// need to delete the state instance??
	PGconn *conn = dstate->conn;
	
	dispatchInputParseAndSend(dstate->conn);
	
	while (conn->asyncStatus == PGASYNC_BUSY) {
		int flushResult;
		while ((flushResult = hgFlush(conn)) > 0) {
			if (hgWait(false, true, conn)) {
				flushResult = -1;
				break;
			}
		}

		if (flushResult ||
			hgWait(true, false, conn) ||
			hgReadData(conn) < 0) {
			//pqSaveErrorResult(conn);
			conn->asyncStatus = PGASYNC_IDLE;
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("fail to handle the connection for dispatch")));
			return false;
		}
		
		dispatchInputParseAndSend(dstate->conn);
		
	}

	switch (conn->asyncStatus) {
	case PGASYNC_IDLE:
		break;
	case PGASYNC_READY:
		// try to get the Z command
		dispatchInputParseAndSend(conn);
		break;
	default:
		ereport(LOG,
				(errmsg("unexpect async status %d", conn->asyncStatus)));
		;
	}
	
	return true;
}

void handleHgSyncloss(PGconn *conn, char id, int msgLength) {
	//need to restart the connection
	ereport(LOG,
			(errmsg("dispatch handle exception case for syncless")));
}


void dispatchInputParseAndSend(PGconn *conn) {
	// only check the parameers which is usefull for dispatch private
	ereport(LOG, (errmsg("dispatch input parse and send enter")));
	char id;
	int msgLength;
	int avail;
	
	for (;;) {
		conn->inCursor = conn->inStart;
		if (hgGetc(&id, conn))
			return;
		if (hgGetInt(&msgLength, 4, conn))
			return;

		/*
		 * Try to validate message type/length here.  A length less than 4 is
		 * definitely broken.  Large lengths should only be believed for a few
		 * message types.
		 */
		if (msgLength < 4)
		{
			handleHgSyncloss(conn, id, msgLength);
			return;
		}
		if (msgLength > 30000 && !VALID_LONG_MESSAGE_TYPE(id))
		{
			handleHgSyncloss(conn, id, msgLength);
			return;
		}

		/*
		 * Can't process if message body isn't all here yet.
		 */
		msgLength -= 4;
		avail = conn->inEnd - conn->inCursor;
		if (avail < msgLength)
		{
			/*
			 * Before returning, enlarge the input buffer if needed to hold
			 * the whole message.  This is better than leaving it to
			 * pqReadData because we can avoid multiple cycles of realloc()
			 * when the message is large; also, we can implement a reasonable
			 * recovery strategy if we are unable to make the buffer big
			 * enough.
			 */
			if (hgCheckInBufferSpace(conn->inCursor + (size_t) msgLength,
									 conn))
			{
				/*
				 * XXX add some better recovery code... plan is to skip over
				 * the message using its length, then report an error. For the
				 * moment, just treat this like loss of sync (which indeed it
				 * might be!)
				 */
				handleHgSyncloss(conn, id, msgLength);
			}
			return;
		}
		
		// do something
		// maybe we can handle the dirty oid here which is send by primary
		// with private id

		switch (id) {
		case 'C':
			conn->asyncStatus = PGASYNC_READY;
			break;
		case 'Z':
			conn->asyncStatus = PGASYNC_IDLE;
			break;
		default:
			//just ignore all the others
			;
		}

		conn->inCursor += msgLength;
		hg_putbytes(conn->inBuffer + conn->inStart, msgLength+5);
		conn->inStart = conn->inCursor;
		
	}
	
}

