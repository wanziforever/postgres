#include "postgres.h"
#include "libpq-fe.h"
#include "libpq/libpq-be.h"
#include "replication/walreceiver.h"
#include "hgdispatch_utility.h"
#include "hgdispatch.h"

extern struct Port *MyProcPort;

static PGconn *Dispatch_Connection = NULL;
extern bool getPrimaryHostInfo(char *host, char* port);

PGconn* createDispatchConnection(void) {
	#define PARAMS_ARRAY_SIZE	8

	const char *keywords[PARAMS_ARRAY_SIZE];
	const char *values[PARAMS_ARRAY_SIZE];
	char primaryHostName[MAXCONNINFO];
	char primaryPort[10];

	PGconn *conn = NULL;

	if (!getPrimaryHostInfo(primaryHostName, primaryPort)) {
		ereport(ERROR, (errmsg("fail to get primary host information!")));
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
	// special application name for dispatch, related check in the postmaster code
	values[5] = "hgdispatch";
	keywords[6] = "client_encoding";
	values[6] = pg_get_client_encoding_name();
	keywords[7] = NULL;
	values[7] = NULL;

	conn = PQconnectdbParams(keywords, values, 1);

	if (PQstatus(conn) == CONNECTION_BAD) {
		ereport(ERROR,
			(errmsg("fail to create dispatch connection, %s",
				PQerrorMessage(conn))));
		PQfinish(conn);
		return NULL;
	}

	return conn;
}


PGconn* getCurrentDispatchConnection(void) {
	//actually need the top level context
	if (Dispatch_Connection == NULL) {
		Dispatch_Connection = createDispatchConnection();
	}
	return Dispatch_Connection;
}


void resetConnOnError(PGconn *conn) {
	/* meet error and reset the connection, for only on error and reset, because
	   refer the resetConn function for libpq, since i cannot directly use the
	   libpq internal function call, and simply just use the pgDropConnection
	   utility function to drop the connection without send the X message.
	*/
	hgDropConnection(conn, true);
	/* it is not good to touch the global variable on the pq wrapper function
	   for hgDropConnection(), so reset the Dispatch_Connection here.
	   actually whether need to create a connection here is a choice. */
	Dispatch_Connection = createDispatchConnection();
}

