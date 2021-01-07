#include "string.h"
#include "postgres.h"
#include "libpq-fe.h"
#include "libpq/libpq-be.h"
#include "libpq/crypt.h"
#include "common/md5.h"
#include "replication/walreceiver.h"
#include "hgdispatch_utility.h"

bool requireDispatch(CommandTag cmdTag, RawStmt* parsetree) {
	// also need to consider the feature lock
	if (!RecoveryInProgress())
		return false;

	//if (strncmp(cmdTag, "SELECT", 6) == 0)
	if (cmdTag == CMDTAG_SELECT)
		return false;

	return true;
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
				(errmsg("%s %s %d return password [%s]",
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
	const char *value = NULL;
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
