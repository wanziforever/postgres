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


/* if there are many oids, consider to use a binary search */
bool addDispatchDirtyOid(Oid oid, int64 ts) {
	if (dirtyOidNum >= MAX_DIRTY_OIDS) {
		return false;
	}

	ereport(LOG, (errmsg("addDispatchDirtyOid for %d, %lu", oid, ts)));
	dirtyOids[dirtyOidNum] = oid;
	dirtyTimestamp[dirtyOidNum] = ts;
	dirtyOidNum++;

	showAllDirtyOids();
	
	return true;
}


int findDispatchDirtyOid(Oid oid, int64 *ts) {
	int i = 0;
	for (; i < dirtyOidNum; i++) {
		if (oid == dirtyOids[i]) {
			*ts = dirtyTimestamp[i];
			return i;
		}
	}
	return -1;
}

void cleanupDispatchDirtyOids(void) {
	ereport(LOG, (errmsg("cleanup Dispatch dirty oids")));
	dirtyOidNum = 0;
}

int getDirtyOidNum(void) {
	return dirtyOidNum;
}

Oid* getDirtyOids(void) {
	return dirtyOids;
}

int64* getDirtyTimestamp(void) {
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
	ereport(LOG, (errmsg("all dirty oids(%d): %s", dirtyOidNum, buf)));
}


/* there is no requirement logic to delete specific oid  */
