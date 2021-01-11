#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "libpq-fe.h"
#include "tcop/cmdtag.h"
#include "lib/stringinfo.h"
#include "pqwrapper.h"


typedef struct {
	PGconn *conn;
} DispatchState;

typedef struct {
	bool isdispatched;
} PrepareQueryDispatched;

bool getPrimaryHostInfo(char *host, char* port);
PGconn* createDispatchConnection(void);
bool primaryDispatch(Node* parsetree, const char* query_string);
DispatchState* createDispatchState(void);
PGconn* getCurrentDispatchConnection(void);
DispatchState* dispatch(const char* query_string);
bool requireDispatch(CommandTag cmdTag, RawStmt* parsetree);
char *get_password(const char *role);
bool requireExtendParseDispatch(const char* query_string);
bool requireExtendBindDispatch(const char* stmt_name);
bool requireExtendExecuteDispatch(const char* portal_name);
bool fetchPrepareQueriesPlanDispatched(const char *stmt_name);
bool fetchPrepareQueriesPortalDispatched(const char *portal_name);
void storePrepareQueriesDispatched(const char *stmt_name, bool need_dispatch);
DispatchState* extendDispatch(char msgtype, StringInfo input_message);
void dispatchInputParseAndSend(PGconn *conn);
bool handleResultAndForward(DispatchState *dstate);
void handleHgSyncloss(PGconn *conn, char id, int msgLength);
void dropUnnamedPrepareDispatch(void);
