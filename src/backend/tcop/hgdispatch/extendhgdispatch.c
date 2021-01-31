#include "postgres.h"
#include "utils/hsearch.h"
#include "lib/stringinfo.h"
#include "libpq-int.h"
#include "hgdispatch.h"

extern bool checkConnection(void);

DMLQueryStragegy unamed_prepare_dispatched = DISPATCH_NONE;
/* currently parse stmtname and portal name use the same hash instance */
static HTAB *prepare_queries_plan_dispatched = NULL;
static HTAB *prepare_queries_portal_dispatched = NULL;
List *prepare_parsetreelist_for_dispatch = NULL; /* store generated parsetree for dispatch */

/* record the extended query execution strategy for later sync strategy, since
   sync message do not take any information for previous actions, we need to
   know where the sync message to send, we track the previous execution command
   as the same with sync */
DMLQueryStragegy execution_strategy_choice = DISPATCH_NONE;
DMLQueryStragegy describe_strategy_choice = DISPATCH_NONE;

static void initPrepareQueryPlanDispatch(void) {
	HASHCTL hash_ctl;
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(PrepareQueryDispatched);

	prepare_queries_plan_dispatched = hash_create("Prepare Queries Plan Dispatch",
											 32,
											 &hash_ctl,
											 HASH_ELEM);
	
}


static void initPrepareQueryPortalDispatch(void) {
	HASHCTL hash_ctl;
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(PrepareQueryDispatched);

	prepare_queries_portal_dispatched = hash_create("Prepare Queries Portal Dispatch",
											 32,
											 &hash_ctl,
											 HASH_ELEM);
	
}


void dropUnnamedPrepareDispatch(void) {
	unamed_prepare_dispatched = false;
}

void storePrepareQueriesPlanDispatched(const char *stmt_name, DMLQueryStragegy s) {
	PrepareQueryDispatched *entry;
	bool found;


	if (stmt_name[0] == '\0') {
		unamed_prepare_dispatched = s;
		return;
	}

	if (!prepare_queries_plan_dispatched)
		initPrepareQueryPlanDispatch();

	entry = (PrepareQueryDispatched*) hash_search(prepare_queries_plan_dispatched,
												  stmt_name,
												  HASH_ENTER,
												  &found);

	//if (found) {
	//	ereport(WARNING,
	//			(errcode(ERRCODE_DUPLICATE_PSTATEMENT),
	//			 errmsg("prepared statement dispatch \"%s\" already exits", stmt_name)));
	//}
	entry->strategy = s;
}


void storePrepareQueriesPortalDispatched(const char *portal_name, DMLQueryStragegy s) {
	PrepareQueryDispatched *entry;
	bool found;

	if (!prepare_queries_portal_dispatched)
		initPrepareQueryPortalDispatch();

	entry = (PrepareQueryDispatched*) hash_search(prepare_queries_portal_dispatched,
												  portal_name,
												  HASH_ENTER,
												  &found);

	//if (found) {
	//	ereport(WARNING,
	//			(errcode(ERRCODE_DUPLICATE_PSTATEMENT),
	//			 errmsg("prepared portal dispatch \"%s\" already exits", portal_name)));
	//}
	entry->strategy = s;

}

DMLQueryStragegy fetchPrepareQueriesPlanDispatched(const char *stmt_name) {
	PrepareQueryDispatched *entry;
	if (prepare_queries_plan_dispatched) {
		entry = (PrepareQueryDispatched *) hash_search(prepare_queries_plan_dispatched,
													   stmt_name,
													   HASH_FIND,
													   NULL);
	} else {
		entry = NULL;
	}


	if (entry == NULL) {
		return DISPATCH_NONE;
	} else {
		return entry->strategy;
	}
	
	return DISPATCH_NONE;
}

DMLQueryStragegy fetchPrepareQueriesPortalDispatched(const char *portal_name) {
	PrepareQueryDispatched *entry;
	if (prepare_queries_portal_dispatched) {
		entry = (PrepareQueryDispatched *) hash_search(prepare_queries_portal_dispatched,
													   portal_name,
													   HASH_FIND,
													   NULL);
	} else {
		entry = NULL;
	}

	if (entry == NULL) {
		return DISPATCH_NONE;
	} else {
		return entry->strategy;
	}
	
	return DISPATCH_NONE;
}


bool extendDispatch(char msgtype, StringInfo input_message) {
	// for simple, without parse the input message and setup the repare message
	// again, i use the input mesage from standby client directly operate on the
	// connction outBuffer directly. but refer to the pqPutMsgStart and
	// pqPutMsgBytes, pqPutMsgEnd functions.

	// cannot use hg_putbytes, becasue libpq has two parts of functionality,
	// one is used for client, and the other is used for send message beck to
	// client, hg_putbytes is belongs to the later one.

	checkConnection();

	PGconn *conn = Dispatch_State.conn;
	
	conn->outBuffer[conn->outCount] = msgtype;
	conn->outMsgEnd = conn->outCount + 1;
	conn->outMsgStart = conn->outMsgEnd;
	conn->outMsgEnd += 4; /* reserve a length space which referenced by outMsgStart */
	uint32 msgLen = input_message->len;
	
	memcpy(conn->outBuffer + conn->outMsgEnd, input_message->data, msgLen);

	conn->outMsgEnd += msgLen;
	
	msgLen = pg_hton32(msgLen+4);
	memcpy(conn->outBuffer + conn->outMsgStart, &msgLen, 4);

	conn->outCount = conn->outMsgEnd;

	if (conn->outCount >= 8192) {
		int toSend = conn->outCount - (conn->outCount % 8192);
		if (hgSendSome(conn, toSend) < 0 )
			return NULL;
	}
	

	if (msgtype == 'P')
	{
		set_dml_read_func_oids_num();
	}

	if (msgtype == 'S') {
		hgFlush(conn);
	}

	conn->asyncStatus = PGASYNC_BUSY;

	// need to delete the state instance??
	return true;
}

// consider to support a given statement name from caller
bool sendDeferTransactionBlockStart(void) {
	//construct a simple begin P/B/E message

	checkConnection();
	
	char buf[64] = {'\0'};
	int pos = 0;
	uint32 len = 0;
	uint32 u32 = 0;

	// parse message
	buf[pos] = 'P';
	pos += 1;
	u32 = pg_hton32(13);
	memcpy(buf+pos, &u32, 4);
	// empty statement name, and length(4)
	pos += 5;
	memcpy(buf+pos, "BEGIN", 5);
	pos += 6;
	// two empty parameters
	pos += 2;

	// bind message
	buf[pos] = 'B';
	pos += 1;
	u32 = pg_hton32(12);
	memcpy(buf+pos, &u32, 4);
	// empty for everything
	pos += 12;

	// execute message
	buf[pos] = 'E';
	pos += 1;
	u32 = pg_hton32(9);
	memcpy(buf+pos, &u32, 4);
	// empty for everything
	pos += 9;

	len = pos;

	PGconn *conn = Dispatch_State.conn;
	conn->outMsgEnd = conn->outCount;
	memcpy(conn->outBuffer + conn->outMsgEnd, buf, len);

	conn->outMsgEnd += len;
	
	conn->outCount = conn->outMsgEnd;

	if (conn->outCount >= 8192) {
		int toSend = conn->outCount - (conn->outCount % 8192);
		if (hgSendSome(conn, toSend) < 0 )
			return NULL;
	}

	conn->asyncStatus = PGASYNC_BUSY;
	
}
