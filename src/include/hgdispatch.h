#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "libpq-fe.h"
#include "tcop/cmdtag.h"
#include "lib/stringinfo.h"
//#include "pqwrapper.h"

typedef enum DMLQueryStragegy {
	DISPATCH_STANDBY,
	DISPATCH_PRIMARY,
	DISPATCH_PRIMARY_AND_STANDBY
} DMLQueryStragegy;

typedef struct {
	PGconn *conn;
	DMLQueryStragegy stragegy;
} DispatchState;

typedef struct {
	char extendName[NAMEDATALEN]; // both for portal name and stmt name
	bool isdispatched;
} PrepareQueryDispatched;

bool getPrimaryHostInfo(char *host, char* port);
PGconn* createDispatchConnection(void);
bool primaryDispatch(Node* parsetree, const char* query_string);
DispatchState* createDispatchState(void);
PGconn* getCurrentDispatchConnection(void);
DispatchState* dispatch(const char* query_string);
DMLQueryStragegy requireDispatch(CommandTag cmdTag, RawStmt* parsetree);
char *get_password(const char *role);
DMLQueryStragegy requireExtendParseDispatch(const char* query_string);
DMLQueryStragegy requireExtendBindDispatch(const char* stmt_name);
DMLQueryStragegy requireExtendExecuteDispatch(const char* portal_name);
bool fetchPrepareQueriesPlanDispatched(const char *stmt_name);
bool fetchPrepareQueriesPortalDispatched(const char *portal_name);
void storePrepareQueriesDispatched(const char *stmt_name, bool need_dispatch);
DispatchState* extendDispatch(char msgtype, StringInfo input_message);
void dispatchInputParseAndSend(PGconn *conn, bool consume_message_only);
bool handleResultAndForward(DispatchState *dstate);
void handleHgSyncloss(PGconn *conn, char id, int msgLength);
void dropUnnamedPrepareDispatch(void);

#define MAX_DIRTY_OIDS 100

int getDirtyOidNum(void);
Oid* getDirtyOids(void);
int64* getDirtyTimestamp(void);
void showAllDirtyOids(void);
int64 getHgGetCurrentLocalSeconds(void);

extern bool am_dml_dispatch;
