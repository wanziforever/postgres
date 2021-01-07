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
