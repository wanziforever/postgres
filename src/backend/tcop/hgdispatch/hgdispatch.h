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
	DISPATCH_NONE,
	DISPATCH_STANDBY,
	DISPATCH_PRIMARY,
	DISPATCH_PRIMARY_AND_STANDBY,
	DISPATCH_TRANSACTION_START_REMOTE_DEFER
} DMLQueryStragegy;

typedef enum DispatchMode {
	DISPATCH_SESSION,
	DISPATCH_TRANSACTION
} DispatchMode;

typedef struct {
	DMLQueryStragegy parse;
	DMLQueryStragegy bind;
	DMLQueryStragegy describe;
	DMLQueryStragegy execute;
	DMLQueryStragegy sync;
} ExtendQueryStrategy;

typedef struct {
	PGconn *conn;
	DMLQueryStragegy strategy;
	DMLQueryStragegy ext_strategy;
	bool simple_begin_defer; // used for simple query
	bool in_transaction_block; // used for extention block, true or false update coorespondingly
	bool remote_begin_been_send;
	bool remote_begin_response_consumed;
	int response_avoid_duplicated_consumed;
} DispatchState;


typedef struct {
	char extendName[NAMEDATALEN]; // both for portal name and stmt name
	DMLQueryStragegy strategy;
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
int dispatchInputParseAndSend(PGconn *conn, int* ignore_msg_num);
bool handleResultAndForward(void);
void handleHgSyncloss(PGconn *conn, char id, int msgLength);
void dropUnnamedPrepareDispatch(void);

void showAllDirtyOids(void);
uint64 getHgGetCurrentLocalSeconds(void);
bool examineDirtyOid(Oid oidl);

bool appendDispatchDirtyOid(Oid oid, uint64 ts);

extern bool am_dml_dispatch;
extern bool enable_dml_dispatch;
extern DispatchMode dispatch_check_scope;

#define MAX_DIRTY_OIDS 50
extern bool enable_dml_dispatch;
extern DispatchState Dispatch_State;
extern DispatchState *hgdstate;
