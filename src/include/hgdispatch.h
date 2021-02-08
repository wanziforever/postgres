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
bool dispatch(const char* query_string);
DMLQueryStragegy requireDispatch(CommandTag cmdTag, RawStmt* parsetree);
char *get_password(const char *role);
DMLQueryStragegy requireExtendParseDispatch(const char* query_string);
DMLQueryStragegy requireExtendBindDispatch(const char* stmt_name);
DMLQueryStragegy requireExtendExecuteDispatch(const char* portal_name);
DMLQueryStragegy fetchPrepareQueriesPlanDispatched(const char *stmt_name);
DMLQueryStragegy fetchPrepareQueriesPortalDispatched(const char *portal_name);
void storePrepareQueriesDispatched(const char *stmt_name, DMLQueryStragegy s);
bool extendDispatch(char msgtype, StringInfo input_message);
int dispatchInputParseAndSend(PGconn *conn, bool consume_message_only);
bool handleResultAndForward(void);
void handleHgSyncloss(PGconn *conn, char id, int msgLength);
void dropUnnamedPrepareDispatch(void);
bool appendDispatchDirtyOid(Oid oid, uint64 ts);

#define MAX_DIRTY_OIDS 100

int getDirtyOidNum(void);
Oid* getDirtyOids(void);
int64* getDirtyTimestamp(void);
void showAllDirtyOids(void);
uint64 getHgGetCurrentLocalSeconds(void);

extern bool am_dml_dispatch;
extern bool enable_dml_dispatch;
extern uint64 dirty_oid_timeout_interval;
