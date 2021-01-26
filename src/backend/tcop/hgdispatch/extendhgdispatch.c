#include "postgres.h"
#include "utils/hsearch.h"
#include "lib/stringinfo.h"
#include "libpq-int.h"
#include "hgdispatch.h"



bool unamed_prepare_dispatched = false;
/* currently parse stmtname and portal name use the same hash instance */
static HTAB *prepare_queries_plan_dispatched = NULL;
static HTAB *prepare_queries_portal_dispatched = NULL;
List *prepare_parsetreelist_for_dispatch = NULL; /* store generated parsetree for dispatch */

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

void storePrepareQueriesPlanDispatched(const char *stmt_name, bool need_dispatch) {
	PrepareQueryDispatched *entry;
	bool found;


	if (stmt_name[0] == '\0') {
		unamed_prepare_dispatched = true;
		//return;
	}

	if (!prepare_queries_plan_dispatched)
		initPrepareQueryPlanDispatch();

	entry = (PrepareQueryDispatched*) hash_search(prepare_queries_plan_dispatched,
												  stmt_name,
												  HASH_ENTER,
												  &found);

	if (found) {
		ereport(WARNING,
				(errcode(ERRCODE_DUPLICATE_PSTATEMENT),
				 errmsg("prepared statement dispatch \"%s\" already exits", stmt_name)));
	}
	entry->isdispatched = need_dispatch;
}


void storePrepareQueriesPortalDispatched(const char *portal_name, bool need_dispatch) {
	PrepareQueryDispatched *entry;
	bool found;

	if (!prepare_queries_portal_dispatched)
		initPrepareQueryPortalDispatch();

	entry = (PrepareQueryDispatched*) hash_search(prepare_queries_portal_dispatched,
												  portal_name,
												  HASH_ENTER,
												  &found);

	if (found) {
		ereport(WARNING,
				(errcode(ERRCODE_DUPLICATE_PSTATEMENT),
				 errmsg("prepared portal dispatch \"%s\" already exits", portal_name)));
	}
	entry->isdispatched = need_dispatch;

}

bool fetchPrepareQueriesPlanDispatched(const char *stmt_name) {
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
		return false;
	} else {
		return entry->isdispatched;
	}
	
	return false;
}

bool fetchPrepareQueriesPortalDispatched(const char *portal_name) {
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
		return false;
	} else {
		return entry->isdispatched;
	}
	
	return false;
}


DispatchState* extendDispatch(char msgtype, StringInfo input_message) {
	// for simple, without parse the input message and setup the repare message
	// again, i use the input mesage from standby client directly operate on the
	// connction outBuffer directly. but refer to the pqPutMsgStart and
	// pqPutMsgBytes, pqPutMsgEnd functions.

	// cannot use hg_putbytes, becasue libpq has two parts of functionality,
	// one is used for client, and the other is used for send message beck to
	// client, hg_putbytes is belongs to the later one.

	DispatchState *dstate = createDispatchState();
	PGconn *conn = dstate->conn;
	
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

	//int toSend = conn->outCount - (conn->outCount % 8192);

	if (hgSendSome(conn, conn->outCount) < 0) {
		ereport(ERROR,
				(errmsg("fail to send extended dispatch message")));
		return NULL;
	}

	if (msgtype == 'P')
	{
		set_dml_read_func_oids_num();
	}

	// need to delete the state instance??
	return dstate;
}
