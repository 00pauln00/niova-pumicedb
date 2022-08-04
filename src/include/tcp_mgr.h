/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Kit Westneat <kit@niova.io> 2020
 */

#ifndef __NIOVA_TCP_MGR_H_
#define __NIOVA_TCP_MGR_H_ 1

#include "epoll_mgr.h"
#include "tcp.h"

#define TCP_MGR_MAX_HDR_SIZE 65000
#define TCP_MGR_MAX_BULK_SIZE 256*1024*1024

typedef void    tcp_mgr_ctx_t;
typedef int     tcp_mgr_ctx_int_t;
typedef ssize_t tcp_mgr_ctx_ssize_t;

struct tcp_mgr_connection;

typedef tcp_mgr_ctx_int_t
(*tcp_mgr_msg_type_cb_t)(struct tcp_mgr_connection *, char *, size_t);
typedef tcp_mgr_ctx_int_t
(*tcp_mgr_recv_cb_t)(struct tcp_mgr_connection *, char *, size_t, void *);
typedef tcp_mgr_ctx_ssize_t
(*tcp_mgr_bulk_size_cb_t)(struct tcp_mgr_connection *, char *, void *, uint32_t *);
typedef tcp_mgr_ctx_int_t
(*tcp_mgr_handshake_cb_t)(void *, struct tcp_mgr_connection **, size_t *,
                          int fd, void *, size_t);
typedef tcp_mgr_ctx_ssize_t
(*tcp_mgr_handshake_fill_t)(void *, struct tcp_mgr_connection *,
                            void *, size_t);
typedef tcp_mgr_ctx_t
(*tcp_mgr_connection_epoll_ctx_cb_t)(struct tcp_mgr_connection *);

struct tcp_mgr_instance
{
    struct tcp_socket_handle tmi_listen_socket;
    void                    *tmi_data;

    struct epoll_mgr        *tmi_epoll_mgr;
    struct epoll_handle      tmi_listen_eph;
    epoll_mgr_ref_cb_t       tmi_connection_ref_cb;
    pthread_mutex_t          tmi_epoll_ctx_mutex;

    tcp_mgr_msg_type_cb_t    tmi_msg_type_cb;
    tcp_mgr_recv_cb_t        tmi_recv_cb;
    tcp_mgr_bulk_size_cb_t   tmi_bulk_size_cb;
    tcp_mgr_handshake_cb_t   tmi_handshake_cb;
    tcp_mgr_handshake_fill_t tmi_handshake_fill;
    size_t                   tmi_handshake_size;

    niova_atomic32_t         tmi_bulk_credits;
    niova_atomic32_t         tmi_incoming_credits;
};

enum tcp_mgr_connection_status
{
    TMCS_NEEDS_SETUP,
    TMCS_DISCONNECTING,
    TMCS_DISCONNECTED,
    TMCS_CONNECTING,
    TMCS_CONNECTED,
};

struct tcp_mgr_connection
{
    enum tcp_mgr_connection_status    tmc_status;
    struct tcp_socket_handle          tmc_tsh;
    struct epoll_handle               tmc_eph;
    struct tcp_mgr_instance          *tmc_tmi;
    size_t                            tmc_header_size;
    char                             *tmc_bulk_buf;
    size_t                            tmc_bulk_offset;
    size_t                            tmc_bulk_remain;
    tcp_mgr_connection_epoll_ctx_cb_t tmc_epoll_ctx_cb;
};

struct tcp_mgr_incoming_connection
{
    niova_atomic8_t           tmic_refcnt;
    struct tcp_mgr_connection tmic_tmc;
};

#define DBG_TCP_MGR_CXN(log_level, tmc, fmt, ...)                    \
do {                                                                 \
   SIMPLE_LOG_MSG(log_level, "tmc[%p]: %s:%d fd=%d " fmt, (tmc),      \
                 (tmc)->tmc_tsh.tsh_ipaddr, (tmc)->tmc_tsh.tsh_port, \
                 (tmc)->tmc_tsh.tsh_socket,                          \
                 ##__VA_ARGS__);                                     \
} while(0)

void
tcp_mgr_setup(struct tcp_mgr_instance *tmi, void *data,
              epoll_mgr_ref_cb_t connection_ref_cb,
              tcp_mgr_recv_cb_t recv_cb,
              tcp_mgr_bulk_size_cb_t bulk_size_cb,
              tcp_mgr_handshake_cb_t handshake_cb,
              tcp_mgr_handshake_fill_t handshake_fill,
              size_t handshake_size, uint32_t bulk_credits,
              uint32_t incoming_credits);

int
tcp_mgr_sockets_close(struct tcp_mgr_instance *tmi);

int
tcp_mgr_sockets_setup(struct tcp_mgr_instance *tmi, const char *ipaddr,
                      int port);

int
tcp_mgr_sockets_bind(struct tcp_mgr_instance *tmi);

int
tcp_mgr_epoll_setup(struct tcp_mgr_instance *tmi, struct epoll_mgr *epoll_mgr,
                    bool is_raft_client);

static inline void
tcp_mgr_connection_header_size_set(struct tcp_mgr_connection *tmc,
                                   size_t size)
{
    tmc->tmc_header_size = size;
}

static inline size_t
tcp_mgr_connection_header_size_get(struct tcp_mgr_connection *tmc)
{
    return tmc->tmc_header_size;
}

int
tcp_mgr_send_msg(struct tcp_mgr_connection *tmc, struct iovec *iov,
                 size_t niovs);

void
tcp_mgr_bulk_credits_set(struct tcp_mgr_instance *tmi, uint32_t cnt);

void
tcp_mgr_incoming_credits_set(struct tcp_mgr_instance *tmi, uint32_t cnt);

void
tcp_mgr_connection_close(struct tcp_mgr_connection *tmc);

// not thread safe for connection
void
tcp_mgr_connection_setup(struct tcp_mgr_connection *tmc,
                         struct tcp_mgr_instance *tmi,
                         const char *ipaddr, int port);

int
tcp_mgr_recv_req_from_socket(struct tcp_mgr_connection *tmc, char *buf, size_t *buff_size);
#endif
