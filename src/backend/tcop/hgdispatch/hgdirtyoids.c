#include "postgres.h"
#include "hgdispatch.h"

/* both primary and standby side will use this buffers for dirty oids, but the
   usage may be different, primary host will use the buffer to hold found dirty oids
   standby host will get the dirty oids data from message, and compare the touched
   tables for select statement
*/

/* give a limitation dirty oid number, the too many dirty */
static Oid dirtyOids[MAX_DIRTY_OIDS];
static int64 dirtyTimestamp[MAX_DIRTY_OIDS];
static int dirtyOidNum = 0;

uint64 dirty_oid_timeout_interval = 10; // seconds

int findDispatchDirtyOid(Oid oid);

/* if there are many oids, consider to use a binary search */
bool addDispatchDirtyOid(Oid oid, uint64 ts) {
	int i = -1;
	if ((i = findDispatchDirtyOid(oid)) >= 0) {
		dirtyOids[i] = ts;
		return true;
	}
	
	if (dirtyOidNum >= MAX_DIRTY_OIDS) {
		return false;
	}

	dirtyOids[dirtyOidNum] = oid;
	dirtyTimestamp[dirtyOidNum] = ts;
	dirtyOidNum++;

	showAllDirtyOids();
	
	return true;
}

/* only return the index of the interval arary */
int findDispatchDirtyOid(Oid oid) {
	int i = 0;
	for (; i < dirtyOidNum; i++) {
		if (oid == dirtyOids[i]) 
			return i;
	}
	return -1;
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

/* there is no requirement logic to delete specific oid  */

/*
  return false for oid is not in dirty list or has already timeout
         true for oid is taking effect
 */
bool examineDirtyOid(Oid oid) {
	int i = -1;
	if ((i = findDispatchDirtyOid(oid)) == -1) {
		return false;
	}

	uint64 curts = getHgGetCurrentLocalSeconds();
	if (dirtyTimestamp[i] + dirty_oid_timeout_interval > curts) {
		//ereport(LOG, (errmsg("examineDirtyOid find the request oid, and it is not timeout (%d, %d)", curts, dirtyTimestamp[i])));
		return true;
	}

	//ereport(LOG, (errmsg("examineDirtyOid find the request oid, and it is timeout (%d, %d)", curts, dirtyTimestamp[i])));
	return false;
}
