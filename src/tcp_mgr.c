#include <sys/ioctl.h>

#include "alloc.h"
#include "log.h"
#include "epoll_mgr.h"
#include "tcp.h"
#include "tcp_mgr.h"
#include "util.h"

#define DEFAULT_BULK_BUFS 32

REGISTRY_ENTRY_FILE_GENERATE;

// XXX should this be on tcp_mgr_instance?
static uint32_t bufsAvail = DEFAULT_BULK_BUFS;

void
set_bulk_bufs_avail(uint32_t cnt)
{
    bufsAvail = cnt;
}

// XXX not particularly thread safe
void *
tcp_mgr_bulk_malloc(size_t sz)
{
    if (bufsAvail <= 0)
        return NULL;

    void *buf = niova_malloc_can_fail(sz);
    if (!buf)
        return NULL;

    bufsAvail--;
    return buf;
}

void
tcp_mgr_bulk_free(void *buf)
{
    niova_free(buf);
    bufsAvail++;
}

void
tcp_mgr_setup(struct tcp_mgr_instance *tmi, void *data,
              tcp_mgr_ref_cb_t connection_ref_cb,
              tcp_mgr_recv_cb_t recv_cb,
              tcp_mgr_handshake_cb_t handshake_cb,
              tcp_mgr_handshake_fill_t handshake_fill,
              size_t handshake_size)
{
    NIOVA_ASSERT(tmi);

    tmi->tmi_data = data;
    tmi->tmi_connection_ref_cb = connection_ref_cb;
    tmi->tmi_recv_cb = recv_cb;
    tmi->tmi_handshake_cb = handshake_cb;
    tmi->tmi_handshake_fill = handshake_fill;
    tmi->tmi_handshake_size = handshake_size;

    pthread_mutex_init(&tmi->tmi_status_mutex, NULL);
}

int
tcp_mgr_sockets_setup(struct tcp_mgr_instance *tmi, const char *ipaddr,
                      int port)
{
    strncpy(tmi->tmi_listen_socket.tsh_ipaddr, ipaddr, IPV4_STRLEN);
    tmi->tmi_listen_socket.tsh_port = port;

    return tcp_socket_setup(&tmi->tmi_listen_socket);
}

int
tcp_mgr_sockets_close(struct tcp_mgr_instance *tmi)
{
    return tcp_socket_close(&tmi->tmi_listen_socket);
}

int
tcp_mgr_sockets_bind(struct tcp_mgr_instance *tmi)
{
    int rc = tcp_socket_bind(&tmi->tmi_listen_socket);
    if (rc)
        tcp_mgr_sockets_close(tmi);

    return rc;
}

void
tcp_mgr_connection_setup(struct tcp_mgr_instance *tmi,
                         struct tcp_mgr_connection *tmc)
{
    tcp_socket_handle_init(&tmc->tmc_tsh);
    tmc->tmc_eph.eph_installed = 0;

    tmc->tmc_tmi = tmi;
    tmc->tmc_status = TMCS_DISCONNECTED;
    tmc->tmc_async_buf = NULL;
    tmc->tmc_async_remain = 0;

    SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_mgr_connection_setup() - tmc %p", tmc);
}

void
tcp_mgr_connection_close(struct tcp_mgr_connection *tmc)
{
    if (tmc->tmc_status != TMCS_CONNECTING && tmc->tmc_status != TMCS_CONNECTED)
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "connection not connected, status: %d",
                        tmc->tmc_status);

    niova_mutex_lock(&tmc->tmc_tmi->tmi_status_mutex);
    tmc->tmc_status = TMCS_DISCONNECTING;
    if (tmc->tmc_eph.eph_installed)
        epoll_handle_del(tmc->tmc_tmi->tmi_epoll_mgr, &tmc->tmc_eph);

    tcp_socket_close(&tmc->tmc_tsh);
    tmc->tmc_status = TMCS_DISCONNECTED;
    niova_mutex_unlock(&tmc->tmc_tmi->tmi_status_mutex);
}

static struct tcp_mgr_connection *
tcp_mgr_incoming_new(struct tcp_mgr_instance *tmi)
{
    struct tcp_mgr_connection *tmc =
        niova_malloc(sizeof(struct tcp_mgr_connection));
    if (!tmc)
        return NULL;

    tcp_mgr_connection_setup(tmi, tmc);

    return tmc;
}

static void tcp_mgr_incoming_fini_err(struct tcp_mgr_connection *tmc)
{
    tcp_mgr_connection_close(tmc);
    niova_free(tmc);
}

static void tcp_mgr_incoming_fini(struct tcp_mgr_connection *tmc)
{
    niova_free(tmc);
}

static int
tcp_mgr_connection_epoll_add(struct tcp_mgr_connection *tmc,
                             uint32_t events,
                             epoll_mgr_cb_t cb,
                             void (*ref_cb)(void *, enum epoll_handle_ref_op))
{
    int rc = epoll_handle_init(&tmc->tmc_eph, tmc->tmc_tsh.tsh_socket,
                               events, cb, tmc, ref_cb);
    if (rc)
        return rc;

    return epoll_handle_add(tmc->tmc_tmi->tmi_epoll_mgr, &tmc->tmc_eph);
}

static int
tcp_mgr_handshake_iov_init(struct tcp_mgr_instance *tmi, struct iovec *iov)
{
    iov->iov_base = niova_malloc_can_fail(tmi->tmi_handshake_size);
    if (!iov->iov_base)
        return -ENOMEM;

    iov->iov_len = tmi->tmi_handshake_size;

    return 0;
}

static void
tcp_mgr_handshake_iov_fini(struct iovec *iov)
{
    NIOVA_ASSERT(iov && iov->iov_base);
    niova_free(iov->iov_base);
}

static int
epoll_handle_rc_get(const struct epoll_handle *eph, uint32_t events)
{
    int rc = 0;

    if (events & (EPOLLHUP | EPOLLERR))
    {
        socklen_t rc_len = sizeof(rc);
        int rc2 = getsockopt(eph->eph_fd, SOL_SOCKET, SO_ERROR, &rc, &rc_len);
        if (rc2)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "Error getting socket error: %d", rc2);
            return rc2;
        }
    }

    return -rc;
}

static int
tcp_mgr_async_read_helper(struct tcp_mgr_connection *tmc)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    void *bulk = tmc->tmc_async_buf + tmc->tmc_async_offset;

    struct iovec iov = {
        .iov_base = bulk,
        .iov_len = tmc->tmc_async_remain,
    };

    ssize_t recv_bytes = tcp_socket_recv(&tmc->tmc_tsh, &iov, 1, NULL, false);

    SIMPLE_LOG_MSG(LL_DEBUG, "recv_bytes=%ld iov_len=%ld", recv_bytes,
                   iov.iov_len);

    if (recv_bytes == -EAGAIN)
        recv_bytes = 0;
    else if (recv_bytes == 0)
        return -ENOTCONN;
    else if (recv_bytes < 0)
        return recv_bytes;

    NIOVA_ASSERT(recv_bytes <= tmc->tmc_async_remain);

    tmc->tmc_async_offset += recv_bytes;
    tmc->tmc_async_remain -= recv_bytes;

    return 0;
}

int
tcp_mgr_async_read(struct tcp_mgr_connection *tmc, size_t bulk_size,
                   void *hdr, size_t hdr_size)
{
    NIOVA_ASSERT(tmc && !tmc->tmc_async_buf && !tmc->tmc_async_remain);
    if (!bulk_size)
        return 0;

    size_t buf_size = hdr_size + bulk_size;

    // buffer free'd in tcp_mgr_recv_cb
    void *buf = tcp_mgr_bulk_malloc(buf_size);
    if (!buf)
    {
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "cannot allocate bulk buf");

        int bytes_avail;
        ioctl(tmc->tmc_tsh.tsh_socket, FIONREAD, &bytes_avail);
        if (bytes_avail == bulk_size)
            buf = niova_malloc_can_fail(buf_size);

        if (!buf)
            return -ENOMEM;
    }

    if (hdr && hdr_size)
        memcpy(buf, hdr, hdr_size);

    tmc->tmc_async_buf = buf;
    tmc->tmc_async_offset = hdr_size;
    tmc->tmc_async_remain = bulk_size;

    int rc = tcp_mgr_async_read_helper(tmc);
    if (rc < 0)
        return rc;

    return 0;
}

static void
tcp_mgr_recv_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_connection *tmc = eph->eph_arg;
    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "received events: %d", events);

    int rc = epoll_handle_rc_get(eph, events);
    if (rc)
    {
        if (rc == -ECONNRESET ||
            rc == -ENOTCONN ||
            rc == -ECONNABORTED)
            tcp_mgr_connection_close(tmc);

        SIMPLE_LOG_MSG(LL_NOTIFY, "error received on socket fd=%d, rc=%d",
                       eph->eph_fd, rc);
        return;
    }

    tcp_mgr_recv_cb_t recv_cb = tmc->tmc_tmi->tmi_recv_cb;
    if (!recv_cb)
    {
        DBG_TCP_MGR_CXN(LL_WARN, tmc, "no recv handler defined");
        return;
    }

    if (tmc->tmc_async_remain)
    {
        rc = tcp_mgr_async_read_helper(tmc);
        if (rc)
        {
            SIMPLE_LOG_MSG(LL_NOTIFY, "cannot complete async read, rc=%d", rc);
            tcp_mgr_connection_close(tmc);
            return;
        }
    }

    recv_cb(tmc, tmc->tmc_tmi->tmi_data);

    if (tmc->tmc_async_buf && !tmc->tmc_async_remain)
    {
        tcp_mgr_bulk_free(tmc->tmc_async_buf);

        tmc->tmc_async_buf = NULL;
        tmc->tmc_async_offset = 0;
    }
}

static void
tcp_mgr_handshake_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);
    NIOVA_ASSERT(eph && eph->eph_arg);
    struct tcp_mgr_connection *tmc = eph->eph_arg;
    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;

    struct iovec iov;
    tcp_mgr_handshake_iov_init(tmi, &iov);

    int rc = tcp_socket_recv(&tmc->tmc_tsh, &iov, 1, NULL, 1);
    if (rc <= 0)
    {
        if (rc == 0)
            SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_recv(): connection closed");
        else
            SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_recv(): %s", strerror(-rc));

        tcp_mgr_handshake_iov_fini(&iov);
        tcp_mgr_incoming_fini_err(tmc);

        return;
    }

    SIMPLE_LOG_MSG(LL_TRACE, "tcp_socket_recv(): rc=%d", rc);

    struct tcp_mgr_connection *new_tmc =
        tmi->tmi_handshake_cb(tmi->tmi_data,
                              tmc->tmc_tsh.tsh_socket,
                              iov.iov_base,
                              rc);
    tcp_mgr_handshake_iov_fini(&iov);

    if (!new_tmc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "handshake error: %d", rc);
        tcp_mgr_incoming_fini_err(tmc);

        return;
    }

    if (tmc->tmc_eph.eph_installed)
        epoll_handle_del(tmi->tmi_epoll_mgr, &tmc->tmc_eph);

    tcp_mgr_connection_epoll_add(new_tmc, EPOLLIN, tcp_mgr_recv_cb,
                                 tmi->tmi_connection_ref_cb);

    SIMPLE_LOG_MSG(LL_TRACE, "handshake validated");
    tcp_mgr_incoming_fini(tmc);
}

static int
tcp_mgr_accept(struct tcp_mgr_instance *tmi, int fd)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct tcp_mgr_connection *tmc = tcp_mgr_incoming_new(tmi);
    if (!tmc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_tcp_incoming_new(): NOMEM");
        return -ENOMEM;
    }

    int rc = tcp_socket_handle_accept(fd, &tmc->tmc_tsh);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_handle_accept(): %d", rc);
        tcp_mgr_incoming_fini_err(tmc);

        return rc;
    }

    tcp_mgr_connection_epoll_add(tmc, EPOLLIN, tcp_mgr_handshake_cb, NULL);

    return 0;
}

static void
tcp_mgr_listen_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_NOTIFY);
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_instance *tmi = eph->eph_arg;

    int rc = tcp_mgr_accept(tmi, eph->eph_fd);
    if (rc < 0)
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_mgr_accept(): %d", rc);
}

int
tcp_mgr_epoll_setup(struct tcp_mgr_instance *tmi, struct epoll_mgr *epoll_mgr)
{
    if (!tmi || !epoll_mgr)
        return -EINVAL;

    tmi->tmi_epoll_mgr = epoll_mgr;

    int rc = epoll_handle_init(&tmi->tmi_listen_eph,
                               tmi->tmi_listen_socket.tsh_socket, EPOLLIN,
                               tcp_mgr_listen_cb, tmi, NULL);

    return rc ? rc :
        epoll_handle_add(epoll_mgr, &tmi->tmi_listen_eph);
}

static int
tcp_mgr_connection_epoll_mod(struct tcp_mgr_connection *tmc, int op,
                             epoll_mgr_cb_t cb)
{
    NIOVA_ASSERT(tmc);

    if (cb)
        tmc->tmc_eph.eph_cb = cb;

    if (op)
        tmc->tmc_eph.eph_events = op;

    return epoll_handle_mod(tmc->tmc_tmi->tmi_epoll_mgr, &tmc->tmc_eph);
};

// XXX lock status mutex at beginning
// XXX split into two functions
static int
tcp_mgr_connect_helper(struct tcp_mgr_connection *tmc)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct iovec handshake_iov;

    tcp_mgr_handshake_iov_init(tmc->tmc_tmi, &handshake_iov);

    int rc = tmc->tmc_tmi->tmi_handshake_fill(tmc->tmc_tmi->tmi_data,
                                              handshake_iov.iov_base,
                                              handshake_iov.iov_len);
    if (rc < 0)
    {
        DBG_TCP_MGR_CXN(LL_WARN, tmc, "handshake failed while connecting");

        tcp_mgr_handshake_iov_fini(&handshake_iov);
        tcp_socket_close(&tmc->tmc_tsh);
        return rc;
    }
    rc = tcp_socket_send(&tmc->tmc_tsh, &handshake_iov, 1);

    tcp_mgr_handshake_iov_fini(&handshake_iov);

    if (rc < 0)
    {
        tcp_socket_close(&tmc->tmc_tsh);
        return rc;
    }

    DBG_TCP_MGR_CXN(LL_DEBUG, tmc, "reinstalling epoll handler");

    rc = tcp_mgr_connection_epoll_mod(tmc, EPOLLIN, tcp_mgr_recv_cb);
    if (rc < 0)
    {
        DBG_TCP_MGR_CXN(LL_WARN, tmc, "error reinstalling epoll handler");
        tcp_socket_close(&tmc->tmc_tsh);
        return rc;
    }

    niova_mutex_lock(&tmc->tmc_tmi->tmi_status_mutex);
    if (tmc->tmc_status == TMCS_CONNECTING)
    {
        tmc->tmc_status = TMCS_CONNECTED;
        rc = 0;
    }
    else
    {
        rc = -EAGAIN;
    }
    niova_mutex_unlock(&tmc->tmc_tmi->tmi_status_mutex);

    if (rc)
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc,
                        "connection status changed while connecting");
    else
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "connection established");

    return rc;
}

static void
tcp_mgr_connect_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_connection *tmc = eph->eph_arg;
    SIMPLE_LOG_MSG(LL_NOTIFY,
                   "tcp_mgr_connect_cb(): connecting to %s:%d[%p]",
                   tmc->tmc_tsh.tsh_ipaddr, tmc->tmc_tsh.tsh_port, tmc);

    int rc = epoll_handle_rc_get(eph, events);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_tcp_connect_cb(): error, rc=%d",
                       rc);
        tcp_socket_close(&tmc->tmc_tsh);
        return;
    }

    tcp_mgr_connect_helper(tmc);
}

// XXX lock the whole function
int
tcp_mgr_connection_get(struct tcp_mgr_instance *tmi,
                       struct tcp_mgr_connection *tmc, const char *ipaddr,
                       int port)
{
    // trying to call setup here is racy
    NIOVA_ASSERT(tmi && tmc && tmc->tmc_status != TMCS_NEEDS_SETUP);

    if (tmc->tmc_status == TMCS_CONNECTED)
        return 0;
    if (!ipaddr || tmc->tmc_status == TMCS_DISCONNECTING)
        return -ENOTCONN;
    if (tmc->tmc_status == TMCS_CONNECTING)
        return -EALREADY;

    NIOVA_ASSERT(tmc->tmc_tsh.tsh_socket < 0);

    int rc = tcp_socket_setup(&tmc->tmc_tsh);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_setup(): %d", rc);
        return rc;
    }

    tcp_socket_handle_set_data(&tmc->tmc_tsh, ipaddr, port);

    rc = tcp_mgr_connection_epoll_add(tmc, EPOLLOUT, tcp_mgr_connect_cb,
                                      NULL);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_mgr_connection_epoll_add(): %d", rc);
        return rc;
    }

    niova_mutex_lock(&tmc->tmc_tmi->tmi_status_mutex);
    if (tmc->tmc_status != TMCS_DISCONNECTED)
        rc = -EAGAIN;
    else
        tmc->tmc_status = TMCS_CONNECTING;
    niova_mutex_unlock(&tmc->tmc_tmi->tmi_status_mutex);

    if (rc < 0)
    {
        DBG_TCP_MGR_CXN(LL_WARN, tmc,
                        "connection status changed, status: %d rc: %d",
                        tmc->tmc_status, rc);
        return rc;
    }

    rc = tcp_socket_connect(&tmc->tmc_tsh);
    if (rc < 0 && rc != -EINPROGRESS)
    {
        tcp_socket_close(&tmc->tmc_tsh);
        SIMPLE_LOG_MSG(LL_WARN, "tcp_socket_connect(): %d", rc );
    }
    else if (rc == -EINPROGRESS)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "waiting for connect callback, fd = %d",
                       tmc->tmc_tsh.tsh_socket);
    }
    else     // rc == 0
    {
        rc = tcp_mgr_connect_helper(tmc);
    }

    return rc;
}
