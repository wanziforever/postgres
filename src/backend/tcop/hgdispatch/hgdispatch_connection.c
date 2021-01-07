#include "postgres.h"
#include "libpq-fe.h"
#include "libpq/libpq-be.h"
#include "replication/walreceiver.h"
#include "hgdispatch_utility.h"
#include "hgdispatch.h"

extern struct Port *MyProcPort;

static PGconn *Dispatch_Connection = NULL;
PGconn* createDispatchConnection();

PGconn* createDispatchConnection() {
	ereport(LOG, (errmsg("create dispatch connection enter")));
	#define PARAMS_ARRAY_SIZE	8

	const char *keywords[PARAMS_ARRAY_SIZE];
	const char *values[PARAMS_ARRAY_SIZE];
	char primaryHostName[MAXCONNINFO];
	char primaryPort[10];

	PGconn *conn = NULL;

	if (!getPrimaryHostInfo(primaryHostName, primaryPort)) {
		ereport(LOG, (errmsg("fail to get primary host information!")));
		return NULL;
	}

	keywords[0] = "host";
	values[0] = primaryHostName;
	keywords[1] = "port";
	values[1] = primaryPort;
	keywords[2] = "user";
	values[2] = MyProcPort->user_name;
	keywords[3] = "password";
	values[3] = get_password(MyProcPort->user_name);
	keywords[4] = "dbname";
	values[4] =  MyProcPort->database_name;
	keywords[5] = "fallback_application_name";
	values[5] = "highgo_dispatch";
	keywords[6] = "client_encoding";
	values[6] = pg_get_client_encoding_name();
	keywords[7] = NULL;
	values[7] = NULL;

	conn = PQconnectdbParams(keywords, values, 1);

	if (PQstatus(conn) == CONNECTION_BAD) {
		ereport(LOG,
			(errmsg("fail to create dispatch connection, %s",
				PQerrorMessage(conn))));
		PQfinish(conn);
		return NULL;
	}

	return conn;
}

PGconn* aaabb = NULL;
PGconn* getCurrentDispatchConnection() {
	//actually need the top level context
	if (Dispatch_Connection == NULL) {
		Dispatch_Connection = createDispatchConnection();
	}
	//return Dispatch_Connection;
	//aaabb = createDispatchConnection();
	//Dispatch_Connection = createDispatchConnection();
	return Dispatch_Connection;
}


