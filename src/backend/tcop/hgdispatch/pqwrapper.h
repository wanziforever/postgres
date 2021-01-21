#include "postgres_fe.h"

#include <signal.h>
#include <fcntl.h>
#include <ctype.h>


#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>

#include <sys/stat.h>

#ifdef ENABLE_THREAD_SAFETY
#include <pthread.h>
#endif

#include "fe-auth.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "libpq/libpq-be.h"

ssize_t hgsecure_write(PGconn *conn, const void *ptr, size_t len);
ssize_t hgsecure_raw_write(Port *port, const void *ptr, size_t len);
ssize_t hgsecure_raw_read(PGconn *conn, void *ptr, size_t len);
void hgDropConnection(PGconn *conn, bool flushInput);
uint16 pg_ntoh16(uint16 x);
uint32 pg_ntoh32(uint32 x);
uint64 pg_ntoh64(uint64 x);
uint32 pg_hton32(uint32 x);
ssize_t hgsecure_read(PGconn *conn, void *ptr, size_t len);
int hgGetc(char *result, PGconn *conn);
int hgGets(PQExpBuffer buf, PGconn *conn);
int hgGetInt(int *result, size_t bytes, PGconn *conn);
int hgCheckOutBufferSpace(size_t bytes_needed, PGconn *conn);
int hgCheckInBufferSpace(size_t bytes_needed, PGconn *conn);
int hgReadData(PGconn *conn);
int hgSendSome(PGconn *conn, int len);
int hgFlush(PGconn *conn);
int hgWait(int forRead, int forWrite, PGconn *conn);
int hgWaitTimed(int forRead, int forWrite, PGconn *conn, time_t finish_time);
int hgReadReady(PGconn *conn);
int hgWriteReady(PGconn *conn);
int hgSocketCheck(PGconn *conn, int forRead, int forWrite, time_t end_time);
int hgSocketPoll(int sock, int forRead, int forWrite, time_t end_time);
void hgDropConnection(PGconn *conn, bool flushInput);
void hg_putbytes(char *buf, int len);

