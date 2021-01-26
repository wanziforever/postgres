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
extern DMLQueryStragegy unamed_prepare_dispatched;
List * pg_parse_query(const char *query_string);

extern uint64 timeout_interval;

char*  dml_write_list = NULL;
char*  dml_read_list = NULL;
//Store oids touched by read functions
int    dml_read_func_oids_num = 0;
Oid    dml_read_func_oids[64] = {0};

static char* is_func_in_list(FuncCall* node, char* list)
{
	if ((list == NULL) || (0 == *list))
        {
                return NULL;
        }
        else
        {
                char* name = strVal(llast(node->funcname));
                if ((name == NULL) || (0 == *name))
                {
                        return NULL;
                }
                ereport(DEBUG5, (errmsg("FWD: Process the Function %s in '%s'", name, list)));
                /* Get Function Name and compare with the list */
                return strstr(list, name);
        }
}

//figures out if the function is in the write function list configured in config file. 
//It returns the position in the list if exists.
static char* is_func_in_write_list(FuncCall* node, char* list)
{
	return is_func_in_list(node, list);
}

void set_dml_read_func_oids_num()
{
	dml_read_func_oids_num = 0;
}

//Stores read table oids in dml_read_func_oids array
//to compare with dirty list so as to identify if it should be forwarded to primary node 
static void extract_read_table_oids(FuncCall * node)
{
	char* last = NULL;
        char* func_name = is_func_in_list(node, dml_read_list);
        if (func_name == NULL)
                return;

        /* Find the localtion of '(',')' */
        while((*func_name) != '(')
        {
                if(((*func_name) == 0) || ((*func_name) == ')'))
                        return;
                func_name++;
        }

	func_name++;
        last = func_name;

	 while((*last) != 0)
        {
                if((*last == ',') || (*last == ')'))
                {
                        char* p = strndup(func_name, last-func_name);
                        Oid oid = RelnameGetRelid(p);
                        elog(DEBUG2, "extract_read_table_oids %s = %d", p, oid);
                        if (OidIsValid(oid))
                        {
                                if(dml_read_func_oids_num >= (sizeof(dml_read_func_oids)/sizeof(Oid)))
                                        return;
                                dml_read_func_oids[dml_read_func_oids_num++] = oid;
                        }
                        free(p);
                        if (*last == ')')
                                return;
                        last++;
                        func_name = last;
                }
                else
                {
                        last++;
                }
        }

}

// return true for update semantics in select
bool further_check_select_semantics(Node *node) {
	if (node == NULL) {
		return false;
	}
	//ereport(LOG, (errmsg("FWD: Process the node %d for select", nodeTag(node))));

	switch (nodeTag(node))
	{
	case T_IntoClause:
	case T_LockingClause:
	case T_InsertStmt:
	case T_DeleteStmt:
	case T_UpdateStmt:
		return true;
	case T_SelectStmt:
	{
		SelectStmt *stmt = (SelectStmt *) node;

		if (further_check_select_semantics((Node *)stmt->intoClause))
			return true;
		if (further_check_select_semantics((Node *)stmt->targetList))
			return true;
		if (further_check_select_semantics((Node *)stmt->fromClause))
			return true;
		if (further_check_select_semantics(stmt->whereClause))
			return true;
		if (further_check_select_semantics((Node *)stmt->groupClause))
			return true;
		if (further_check_select_semantics(stmt->havingClause))
			return true;
		if (further_check_select_semantics((Node *)stmt->windowClause))
			return true;
		if (further_check_select_semantics((Node *)stmt->valuesLists))
			return true;
		if (further_check_select_semantics((Node *)stmt->sortClause))
			return true;
		if (further_check_select_semantics(stmt->limitOffset))
			return true;
		if (further_check_select_semantics(stmt->limitCount))
			return true;
		if (further_check_select_semantics((Node *)stmt->lockingClause))
			return true;
		if (further_check_select_semantics((Node *)stmt->withClause))
			return true;
		if (further_check_select_semantics((Node *)stmt->larg))
			return true;
		if (further_check_select_semantics((Node *)stmt->rarg))
			return true;
	}
	break;
	case T_FuncCall:
		if (is_func_in_write_list((FuncCall*)node, dml_write_list) != NULL)	
			return true;
		extract_read_table_oids((FuncCall*)node);
		
		
//		if ((fwd_white_list == NULL) || (0 == *fwd_white_list))
//		{
//			return true;
//		}
//		else
//		{
//			char* name = strVal(llast(((FuncCall *) node)->funcname));
//			if ((name == NULL) || (0 == *name))
//			{
//				return true;
//			}
//			ereport(DEBUG5, (errmsg("FWD: Process the Function %s in '%s'", name, fwd_white_list)));
//			/* Get Function Name and compare with the list */
//			return (NULL == strstr(fwd_white_list, name));
//		}
//
//		break;
	case T_OnConflictClause:
	{
		OnConflictClause *stmt = (OnConflictClause *) node;

		if (further_check_select_semantics((Node *)stmt->infer))
			return true;
		if (further_check_select_semantics((Node *)stmt->targetList))
			return true;
		if (further_check_select_semantics((Node *)stmt->whereClause))
			return true;
	}
	break;
	case T_ResTarget:
	{
		ResTarget  *rt = (ResTarget *) node;
		/* Select Function: The SelectStmt->ListStmt->ResTarget->Function */

		if (further_check_select_semantics((Node *)rt->indirection))
			return true;
		if (further_check_select_semantics(rt->val))
			return true;
	}
	break;
	case T_List:
	{
		ListCell   *temp;
		foreach(temp, (List *) node)
		{
			if (further_check_select_semantics((Node *) lfirst(temp)))
				return true;
		}
	}
	break;
	/* fromlist->T_List and then each item is a RangeVar, or subselect or joinExpr
	   and then expand the subsleect and JoinExpr, RangeVar is the final */
	case T_RangeVar:
	{
		RangeVar *rv = (RangeVar*) node;
		char *relname = rv->relname;
		Oid relid = RelnameGetRelid(relname);
		int ind;

		if (examineDirtyOid(relid)) {
			return true;
		}
	}
	break;
		
	case T_RangeSubselect:
	{
		RangeSubselect *r = (RangeSubselect*)node;
		further_check_select_semantics(r->subquery);
	}
	break;
	case T_JoinExpr:
	{
		/* first left tree, and then right tree */
		JoinExpr *j = (JoinExpr *)node;
		if (further_check_select_semantics(j->larg))
			return true;
		if (further_check_select_semantics(j->rarg))
			return true;
	}
	break;
	break;
	default:
		break;
	}

	return false;
}



DMLQueryStragegy requireDispatch(CommandTag cmdTag, RawStmt* parsetree) {
	// also need to consider the feature lock
	switch (cmdTag) {
	case CMDTAG_SELECT:
	{
		/* actually copy to will fail to write to standby, but please don't run copy to standby */
		/* handle for update series cmd */
		bool update_semantics_in_select = false;;
		update_semantics_in_select = further_check_select_semantics(parsetree->stmt);
		if (update_semantics_in_select) {
			return DISPATCH_PRIMARY;
		} else {
			return DISPATCH_STANDBY;
		}
		break;
	}
	case T_CopyStmt: 
		return DISPATCH_STANDBY;
		/* following for transaction tags */
	case CMDTAG_BEGIN:
	case CMDTAG_START_TRANSACTION:
	case CMDTAG_COMMIT:
	case CMDTAG_ROLLBACK:
	case CMDTAG_SAVEPOINT:
	case CMDTAG_RELEASE:
	case CMDTAG_PREPARE_TRANSACTION: /* need attention */
	case CMDTAG_COMMIT_PREPARED:
	case CMDTAG_ROLLBACK_PREPARED:
		/* variable set command will duplicated */
	case CMDTAG_SET:
	case CMDTAG_RESET:
		return DISPATCH_PRIMARY_AND_STANDBY;
	default:
		return DISPATCH_PRIMARY;
	}
	
	// actually still need to consider the CMDTAG_PREPARE and CMDTAG_EXECUTE
	// and CMDTAG_SELECT_FOR_UPDATE series tags
	// it is a little complex.

	return DISPATCH_PRIMARY;
}


DMLQueryStragegy requireExtendParseDispatch(const char* query_string) {
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

DMLQueryStragegy requireExtendBindDispatch(const char* stmt_name) {
	if (stmt_name[0] == '\0') {
		return unamed_prepare_dispatched;
	}
	
	return fetchPrepareQueriesPlanDispatched(stmt_name);
}

DMLQueryStragegy requireExtendExecuteDispatch(const char* portal_name) {
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
		//ereport(LOG, (errmsg("getPrimaryHostInfo %s", token)));
		token = strtok(NULL, " ");
	}

	return found;
}

uint64 getHgGetCurrentLocalSeconds(void) {
	struct timeval tp;
	gettimeofday(&tp, NULL);
	int64 secs = tp.tv_sec;
	return secs;
}
