#include "postgres.h"
#include "utils/hsearch.h"
#include "hgdispatch.h"

/* dirty oid management function will be different on primary and standby end,
   each side consider the its own data managment requirement, the array is easy
   to go through and cleanup, and hash structure is easy to search, and dynamicly
   increase the storage size

   normally the hash structure data will use the process top level memorycontext
   so it can remain there when every the query status.
*/

/* used for primary end, easy to maintain and fast to loop all the oid, since
   primary side code has ability to go through to all the dirty oids, and clean
   all of them, give a limitation dirty oid number, the too many dirty */
static Oid dirtyOids[MAX_DIRTY_OIDS];
static int64 dirtyTimestamp[MAX_DIRTY_OIDS];
static int dirtyOidNum = 0;

int findDispatchDirtyOid(Oid oid);

/* hash table structure used for backup end, since the backend side code only
   need to look up the one or some specified oid */
static HTAB *dirtyoid_store = NULL;

uint64 dirty_oid_timeout_interval = 10; // seconds

typedef struct {
	Oid oid;
	uint64 ts;
} DirtyOidHashEntry;


// code for primary side for dirty oid maintan
// there is no requirement logic to delete specific oid 
bool appendDispatchDirtyOid(Oid oid, uint64 ts) {
	/* the function will not consider the duplicated case, since it is used for
	   primary side dirty oid, and there are not much duplicated oid happend in
	   a SQL execution context, although the duplicated oid will handled by
	   standby side hash structure. */
	if (dirtyOidNum >= MAX_DIRTY_OIDS) {
		return false;
	}

	dirtyOids[dirtyOidNum] = oid;
	dirtyTimestamp[dirtyOidNum] = ts;
	dirtyOidNum++;

	showAllDirtyOids();
	
	return true;
}

void cleanupDispatchDirtyOids(void) {
	//ereport(LOG, (errmsg("cleanup Dispatch dirty oids")));
	dirtyOidNum = 0;
}

int getDirtyOidNum(void) {
	return dirtyOidNum;
}

Oid* getDirtyOids(void) {
	return dirtyOids;
}

uint64* getDirtyTimestamp(void) {
	return dirtyTimestamp;
}


void showAllDirtyOids(void) {
	// MAX_DIRTY_OIDS * (string size of oid and timestamp and some arrow chars)
#define SHOW_DIRTY_BUF_LENGTH (MAX_DIRTY_OIDS * (5+10+4))
	char buf[SHOW_DIRTY_BUF_LENGTH];
	memset(buf, 0, SHOW_DIRTY_BUF_LENGTH);
	char *s = buf;
	int i = 0;

	// only show first 10 dirty oids for difficult to determine the max length
	int loop = dirtyOidNum > 10 ? 10 : dirtyOidNum;
	
	for (; i < loop; i++) {
		sprintf(s, "%d-%lu,", dirtyOids[i], dirtyTimestamp[i]);
		s += strlen(buf);
	}
}

// code for standby side 
static void initDirtyOidStore() {
	HASHCTL hash_ctl;
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(DirtyOidHashEntry);
	dirtyoid_store = hash_create("Dirty Oid Store",
								 100,
								 &hash_ctl,
								 HASH_ELEM | HASH_BLOBS);
}

bool markDispatchDirtyOid(Oid oid, uint64 ts) {
	DirtyOidHashEntry* entry;
	bool found;
	
	if (!dirtyoid_store)
		initDirtyOidStore();

	entry = (DirtyOidHashEntry*) hash_search(dirtyoid_store,
											 &oid,
											 HASH_ENTER,
											 &found);
	entry->ts = ts;
	return true;
}

/*
  return false for oid is not in dirty list or has already timeout
         true for oid is taking effect
 */
bool examineDirtyOid(Oid oid) {
	uint64 curts = getHgGetCurrentLocalSeconds();
	DirtyOidHashEntry* entry = NULL;

	if (!dirtyoid_store)
		return false;

	entry = (DirtyOidHashEntry*) hash_search(dirtyoid_store,
											 &oid,
											 HASH_FIND,
											 NULL);

	if (!entry) {
		return false;
	}
	
	if (entry->ts + dirty_oid_timeout_interval > curts) {
		return true;
	}

	return false;
}

