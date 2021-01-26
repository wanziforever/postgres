#include "hgdispatch.h"
#include "libpq-int.h"


//static DispatchPipline *Dispatch_Pipline = NULL;

bool enable_dml_dispatch = true;
//DispatchMode dispatch_check_scope = DISPATCH_SESSION;
DispatchMode dispatch_check_scope = DISPATCH_TRANSACTION;

#define BUFFER_LEN 4096
#define offsetof(type, field) ((unsigned long)&(((type *)0)->field))
#define IOBUF_SIZE (offsetof(IOBuf, buf) + BUFFER_LEN)
#define VALID_LONG_MESSAGE_TYPE(id) \
	((id) == 'T' || (id) == 'D' || (id) == 'd' || (id) == 'V' || \
	 (id) == 'E' || (id) == 'N' || (id) == 'A')

#define MAX_DISPATCH_RETRY 3

// used to debug the return message char by char examination
//void printhex(char *s, int len) {
//	int i = 0;
//	ereport(LOG, (errmsg("print hex charactors: ")));
//	for (; i < len; i++) {
//		ereport(LOG, (errmsg("%X", s[i])));
//	}
//}


bool am_dml_dispatch = false;


DispatchState* createDispatchState(void) {
	DispatchState *st = palloc(sizeof(DispatchState));
	// here i igore the PQsendStart operation for simple
	PGconn *c = getCurrentDispatchConnection();
	st->conn = c;
	return st;
}

int remote_exec(PGconn *conn, char *query_string) {
	/* normally we can just simply use the PQexec function call to send message,
	   but the PQexec include the get result function which is not what we want.
	   so we have to implement our result function by ourself, and seperate the
	   message send function for flexible use for extended query */

	int n = PQsendQuery(conn, query_string);
	// ignore the cancel operation for simple
	return n;
}


DispatchState* dispatch(const char* query_string) {
	// need to pay attention to free the DispatchState instance
	int retrycount = 0;
	int n = 0;
	DispatchState *dstate = NULL;
	
	do {
		/* some time the connection will have problems, and we need to retry
		   the message send */
		dstate = createDispatchState();
		n = remote_exec(dstate->conn, query_string);
		if (n <= 0) { // the PQsendQuery will return 0 for pqFlush error
			retrycount++;
			if (retrycount >= MAX_DISPATCH_RETRY) {
				ereport(ERROR,
						(errmsg("dispatch send message fail after retry")));
				return NULL;
			}				
			ereport(WARNING,
					(errmsg("dispatch send message error, setup connection again(%d)", retrycount)));
			resetConnOnError(dstate->conn);
			pfree(dstate);
			continue;
		}
	} while (retrycount);

	//ereport(LOG, (errmsg("after pqexec")));
	// need to delete the state instance??
	return dstate;
}

bool handleResultAndForward(DispatchState *dstate) {
	PGconn *conn = dstate->conn;

	bool consume_message_only = false;
	consume_message_only = dstate->stragegy == DISPATCH_PRIMARY_AND_STANDBY ? true:false;
		
	dispatchInputParseAndSend(dstate->conn, consume_message_only);
	
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
		
		dispatchInputParseAndSend(dstate->conn, consume_message_only);
		
	}

	switch (conn->asyncStatus) {
	case PGASYNC_IDLE:
		break;
	case PGASYNC_READY:
		// try to get the Z command
		dispatchInputParseAndSend(conn, consume_message_only);
		break;
	default:
		ereport(ERROR,
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


void dispatchInputParseAndSend(PGconn *conn, bool consume_message_only) {
	// only check the parameers which is usefull for dispatch private
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
		case 'O':
			showAllDirtyOids();
			/* there is no oid number data, the number should be computed base
			   on the parameter message length*/
			int dirtyoidnum = 0;
			if ((msgLength % (sizeof(Oid) + sizeof(int64))) != 0) {
				ereport(ERROR, (errmsg("fail to parse the dirty oids for invalid message length %d", msgLength)));
			}
			dirtyoidnum = msgLength / (sizeof(Oid) + sizeof(int64));
			int i = 0;

			for (; i < dirtyoidnum; i++) {
				char *s = conn->inBuffer + conn->inCursor + i * (sizeof(Oid) + sizeof(int64));
				Oid oid = (Oid)pg_ntoh32(*(Oid*)s);
				s += sizeof(Oid);
				int64 ts = pg_ntoh64(*(uint64*)s);
				/* we do not use the timestamp send from the primary host,
				   because we cannot depend on the time on the primary host,
				   we record the timestamp in standby local, it is OK to do so
				   because we also get this time to do compare with the local
				   request timestamp, the only minor problem is when there is
				   a lot of data received from the primary, and the timestamp
				   in the local will be big lage from primary, but for a update
				   scenario, the return value always to be small most of the time.
				*/
				int64 myts = getHgGetCurrentLocalSeconds();
				addDispatchDirtyOid(oid, myts);
			}

			break;
			
		default:
			//just ignore all the others
			;
		}

		conn->inCursor += msgLength;
		
		// ignore the Z, becasue the standby logic will send a Z still
		if (!consume_message_only && id != 'Z' && id != 'O') 
			hg_putbytes(conn->inBuffer + conn->inStart, msgLength+5);
		
		conn->inStart = conn->inCursor;
		
	}
	
}

