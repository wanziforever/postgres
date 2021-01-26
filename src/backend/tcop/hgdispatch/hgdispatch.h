#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "libpq-fe.h"
#include "tcop/cmdtag.h"
#include "lib/stringinfo.h"
#include "pqwrapper.h"


#define USE_HIGHGO_DISPATCH (enable_dml_dispatch && RecoveryInProgress())

typedef enum DMLQueryStragegy {
	DISPATCH_STANDBY,
	DISPATCH_PRIMARY,
	DISPATCH_PRIMARY_AND_STANDBY
} DMLQueryStragegy;

typedef enum DispatchMode {
	DISPATCH_SESSION,
	DISPATCH_TRANSACTION
} DispatchMode;

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

void showAllDirtyOids(void);
uint64 getHgGetCurrentLocalSeconds(void);
bool examineDirtyOid(Oid oidl);

extern bool am_dml_dispatch;
extern bool enable_dml_dispatch;
extern DispatchMode dispatch_check_scope;

#define MAX_DIRTY_OIDS 50
extern bool enable_dml_dispatch;
