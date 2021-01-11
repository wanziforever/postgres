#include "string.h"
#include "postgres.h"
#include "libpq-fe.h"
#include "libpq/libpq-be.h"
#include "libpq/crypt.h"
#include "common/md5.h"
#include "tcop/utility.h"
#include "replication/walreceiver.h"
#include "hgdispatch.h"

extern List *prepare_parsetreelist_for_dispatch;
extern bool unamed_prepare_dispatched;
List * pg_parse_query(const char *query_string);

bool requireDispatch(CommandTag cmdTag, RawStmt* parsetree) {
	// also need to consider the feature lock
	if (!RecoveryInProgress())
		return false;

	if (cmdTag == CMDTAG_SELECT)
		return false;

	return true;
}


bool requireExtendParseDispatch(const char* query_string) {
	// we do parse the query for the sql statement without wait for doing it in
	// the exec_parse_message. it is a hard decision, becase parse, bind, execute
	// are important part, but different handling code style, it is hard to do
	// my code in a consistent way, but try best to balance the consistent and
	// performance.
	// exec_parse_message() function accept the tokens which parsed from the
	// input message (stmt_name, query_string, params) as parameters, if i do
	// my work in this function, i will not get the input_message directly, this
	// means if i decide to dispatch the parse message, i need to re-setup the
	// the input message again, the code is dirty.and also in this function ,
	// and unnamed change checking is before the parse work, so that measn that
	// the dispatch will happen after clear the local unnamed pointer, it is not
	// good. and i also can mark a global variable in exec_parse_message()
	// function, and after that function to evaluate the global variable, and
	// decide to dispatch. this is not quite dirty, but the code will be splited
	// to two parts, not pretty good. so i decide to to it current way. just do
	// the parse work here, and in order to not do it twice, i need to send the
	// parsetree back to the exec_parse_message function, and if the parse work
	// has been done, the exec_parse_message will not need to do it again.

	List *parsetree_list = pg_parse_query(query_string);

	if (parsetree_list == NULL) {
		return false;
	}

	prepare_parsetreelist_for_dispatch = parsetree_list;
	
	RawStmt *raw_parse_tree = linitial_node(RawStmt, parsetree_list);
	
	CommandTag cmdTag = CreateCommandTag(raw_parse_tree->stmt);
	return requireDispatch(cmdTag, raw_parse_tree);
}

bool requireExtendBindDispatch(const char* stmt_name) {
	if (!RecoveryInProgress())
		return false;

	if (stmt_name[0] == '\0') {
		return unamed_prepare_dispatched;
	}
	
	return fetchPrepareQueriesPlanDispatched(stmt_name);
}

bool requireExtendExecuteDispatch(const char* portal_name) {
	if (!RecoveryInProgress())
		return false;
	
	return fetchPrepareQueriesPortalDispatched(portal_name);
}


char *get_password(const char *role)
{
	char		*logdetail = NULL;
	char		*shadow_pass = NULL;
	char		*crypt_pwd = NULL;
	PasswordType	pwtype;

	/* First look up the user's password. */
	shadow_pass = get_role_password(role, &logdetail);

	if (!shadow_pass)
	{
		ereport(LOG,
		        (errmsg("%s %d %s",
		         __func__, __LINE__, logdetail)));	
		
		return NULL;
	}

	pwtype = get_password_type(shadow_pass);

	switch (pwtype)
	{
		case PASSWORD_TYPE_PLAINTEXT:
			crypt_pwd = palloc(MD5_PASSWD_LEN + 1);

			if (!pg_md5_encrypt(shadow_pass, role, strlen(role), crypt_pwd))
			{
				pfree(crypt_pwd);
				return NULL;
			}

			ereport(LOG,
				(errmsg("%s %d return password [%s]",
						__func__, __LINE__, crypt_pwd)));

			return crypt_pwd;

		case PASSWORD_TYPE_MD5:
			ereport(LOG,
				(errmsg("%s %d return password [%s]",
					 __func__, __LINE__, shadow_pass)));

			return shadow_pass;

		case PASSWORD_TYPE_SCRAM_SHA_256:
		default:
			ereport(LOG,
				(errmsg("%s %d not support the type [%d] of password [%s]",
					__func__, __LINE__, pwtype, shadow_pass)));
			
			return NULL;
	}

	return NULL;
}

bool getPrimaryHostInfo(char *host, char* port) {
	// parse the primary host information from PrimaryConnInfo GUC property
	// is there a case the host name not provided, and use unix-net type
	// connection instead, and port not provided, use 5432 as default
	
	const char *str = PrimaryConnInfo;
	const char *token = NULL;
	token = strtok(str, " ");
	bool found = false;
	while(token != NULL) {
		if (strncmp(token, "host", 4) == 0) {
			strncpy(host, token+5, MAXCONNINFO);
			found = true;
		} else if (strncmp(token, "port", 4) == 0) {
			strncpy(port, token+5, 10);
		}
		ereport(LOG, (errmsg("getPrimaryHostInfo %s", token)));
		token = strtok(NULL, " ");
	}

	return found;
}
