#include "postgres.h"
#include "nodes/nodes.h"
#include "libpq-fe.h"

typedef struct {
	PGconn *conn;
} DispatchState;

bool primaryDispatch(Node* parsetree, const char* query_string);
DispatchState* CreateDispatchState();
PGconn* getCurrentDispatchConnection();
