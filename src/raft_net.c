/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <fcntl.h>
#include <linux/limits.h>
#include <regex.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <unistd.h>

#include "alloc.h"
#include "crc32.h"
#include "ctl_svc.h"
#include "ctor.h"
#include "epoll_mgr.h"
#include "init.h"
#include "io.h"
#include "log.h"
#include "raft.h"
#include "random.h"
#include "regex_defines.h"
#include "udp.h"
#include "util_thread.h"

enum raft_net_lreg_values
{
    RAFT_NET_LREG_IGNORE_TIMER_EVENTS,
    RAFT_NET_LREG__MAX,
};

struct raft_instance raftInstance = {
//    .ri_store_type = RAFT_INSTANCE_STORE_ROCKSDB,
    .ri_store_type = RAFT_INSTANCE_STORE_POSIX_FLAT_FILE,
};

static regex_t raftNetRncuiRegex;

REGISTRY_ENTRY_FILE_GENERATE;

static util_thread_ctx_reg_int_t
raft_net_lreg_multi_facet_cb(enum lreg_node_cb_ops, struct lreg_value *,
                             void *);

//LREG_ROOT_ENTRY_GENERATE(raft_net, LREG_USER_TYPE_RAFT_NET);

LREG_ROOT_ENTRY_GENERATE_OBJECT(raft_net_info, LREG_USER_TYPE_RAFT_NET,
                                RAFT_NET_LREG__MAX,
                                raft_net_lreg_multi_facet_cb, NULL);

struct raft_instance *
raft_net_get_instance(void)
{
    // Xxx this needs to become more flexible so that more than 1 instance
    //      may be serviced by a single process
    return &raftInstance;
}

static util_thread_ctx_reg_int_t
raft_net_lreg_multi_facet_cb(enum lreg_node_cb_ops op, struct lreg_value *lv,
                             void *arg)
{
    if (arg)
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= RAFT_NET_LREG__MAX)
        return -ERANGE;

    struct raft_instance *ri = raft_net_get_instance();
    NIOVA_ASSERT(ri);

    int rc = 0;
    bool tmp_bool = false;

    switch (op)
    {
    case LREG_NODE_CB_OP_READ_VAL:
        switch (lv->lrv_value_idx_in)
        {
        case RAFT_NET_LREG_IGNORE_TIMER_EVENTS:
            lreg_value_fill_bool(lv, "ignore_timer_events",
                                 ri->ri_ignore_timerfd ? true : false);
            break;
        default:
            rc = -ENOENT;
            break;
        }
        break;
    case LREG_NODE_CB_OP_WRITE_VAL:
        if (lv->put.lrv_value_type_in != LREG_VAL_TYPE_STRING)
            return -EINVAL;

        rc = niova_string_to_bool(LREG_VALUE_TO_IN_STR(lv), &tmp_bool);
        if (rc)
            return rc;

        switch (lv->lrv_value_idx_in)
        {
        case RAFT_NET_LREG_IGNORE_TIMER_EVENTS:
            ri->ri_ignore_timerfd = tmp_bool;
            break;
        default:
            rc = -EPERM;
            break;
        }
        break;
    default:
        rc = -EOPNOTSUPP;
        break;
    }

    return rc;
}

static int
raft_net_udp_sockets_close(struct raft_instance *ri)
{
    int rc = 0;

    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX; i++)
    {
        int tmp_rc = udp_socket_close(&ri->ri_ush[i]);
        if (tmp_rc && !rc) // store the first error found.
            rc = tmp_rc;
    }

    return rc;
}

int
raft_net_udp_sockets_bind(struct raft_instance *ri)
{
    int rc = 0;

    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX && !rc; i++)
    {
        if (raft_instance_is_client(ri) && i == RAFT_UDP_LISTEN_SERVER)
            continue;

        rc = udp_socket_bind(&ri->ri_ush[i]);
    }

    if (rc)
        raft_net_udp_sockets_close(ri);

    return rc;
}

int
raft_net_udp_sockets_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = 0;

    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX; i++)
    {
        strncpy(ri->ri_ush[i].ush_ipaddr,
                ctl_svc_node_peer_2_ipaddr(ri->ri_csn_this_peer), IPV4_STRLEN);

        if (i == RAFT_UDP_LISTEN_SERVER) // server <-> server comms port
        {
            if (raft_instance_is_client(ri))
                continue; // no server listen port in client mode

            ri->ri_ush[i].ush_port =
                ctl_svc_node_peer_2_port(ri->ri_csn_this_peer);
        }
        else if (i == RAFT_UDP_LISTEN_CLIENT) // client <-> server port
        {
            ri->ri_ush[i].ush_port =
                ctl_svc_node_peer_2_client_port(ri->ri_csn_this_peer);
        }
        else
        {
            rc = -ESOCKTNOSUPPORT;
            break;
        }

        if (!ri->ri_ush[i].ush_port)
        {
            rc = -ENOENT;
            break;
        }

        rc = udp_socket_setup(&ri->ri_ush[i]);
        if (rc)
            break;
    }

    if (rc)
        raft_net_udp_sockets_close(ri);

    return rc;
}

static int
raft_net_timerfd_create(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    ri->ri_timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (ri->ri_timer_fd < 0)
        return -errno;

    return 0;
}

static int
raft_net_timerfd_close(struct raft_instance *ri)
{
    if (!ri)
	return -EINVAL;

    if (ri->ri_timer_fd >= 0)
    {
        ri->ri_timer_fd = -1;
        return close(ri->ri_timer_fd);
    }

    return 0;
}

static int
raft_net_epoll_cleanup(struct raft_instance *ri)
{
    for (enum raft_epoll_handles i = 0; i < RAFT_EPOLL_HANDLES_MAX; i++)
        epoll_handle_del(&ri->ri_epoll_mgr, &ri->ri_epoll_handles[i]);

    return epoll_mgr_close(&ri->ri_epoll_mgr);
}

/**
 *  raft_net_udp_cb - forward declaration for the generic udp recv handler.
 */
static raft_net_udp_cb_ctx_t
raft_net_udp_cb(const struct epoll_handle *);

static int
raft_net_epoll_handle_add(struct raft_instance *ri, int fd, epoll_mgr_cb_t cb)
{
    if (!ri || fd < 0 || !cb)
        return -EINVAL;

    else if (ri->ri_epoll_handles_in_use >= RAFT_EPOLL_HANDLES_MAX)
        return -ENOSPC;

    size_t idx = ri->ri_epoll_handles_in_use++;

    int rc =
        epoll_handle_init(&ri->ri_epoll_handles[idx], fd, EPOLLIN, cb, ri);

    return rc ? rc :
	epoll_handle_add(&ri->ri_epoll_mgr, &ri->ri_epoll_handles[idx]);
}

static int
raft_epoll_setup_udp(struct raft_instance *ri,
                     enum raft_udp_listen_sockets ruls)
{
    if (!ri ||
        (ruls != RAFT_UDP_LISTEN_SERVER && ruls != RAFT_UDP_LISTEN_CLIENT))
        return -EINVAL;

    return raft_net_epoll_handle_add(ri, ri->ri_ush[ruls].ush_socket,
                                     raft_net_udp_cb);
}

int
raft_net_evp_add(struct raft_instance *ri, epoll_mgr_cb_t cb)
{
    if (!ri || !cb)
        return -EINVAL;

    else if (ri->ri_epoll_handles_in_use >= RAFT_EPOLL_HANDLES_MAX ||
             ri->ri_evps_in_use >= RAFT_EVP_HANDLES_MAX)
        return -ENOSPC;

    int idx = ri->ri_evps_in_use++;

    struct ev_pipe *evp = &ri->ri_evps[idx];

    int rc = ev_pipe_setup(evp);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "ev_pipe_setup(): %s", strerror(-rc));
        return rc;
    }

    rc = raft_net_epoll_handle_add(ri, evp_read_fd_get(evp), cb);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_epoll_handle_add(): %s",
                       strerror(-rc));
        return rc;
    }

    return idx;
}

raft_net_timerfd_cb_ctx_t
raft_net_timerfd_cb(const struct epoll_handle *);

static int
raft_net_epoll_setup_timerfd(struct raft_instance *ri)
{
    if (!ri ||
        (!raft_instance_is_client(ri) && !ri->ri_timer_fd_cb))
        return -EINVAL; // Servers must have specified ri_timer_fd_cb

    else if (!ri->ri_timer_fd_cb)
        return 0;

    return raft_net_epoll_handle_add(ri, ri->ri_timer_fd, raft_net_timerfd_cb);
}

static int
raft_net_epoll_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = epoll_mgr_setup(&ri->ri_epoll_mgr);
    if (rc)
        return rc;

    /* Add the timerfd to the epoll_mgr.
     */
    rc = raft_net_epoll_setup_timerfd(ri);

    /* Next, add the udp sockets.
     */
    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX && !rc; i++)
    {
        if (raft_instance_is_client(ri) && i == RAFT_UDP_LISTEN_SERVER)
            continue;

        rc = raft_epoll_setup_udp(ri, i);
    }

    if (rc)
        raft_net_epoll_cleanup(ri);

    return rc;
}

static void
raft_net_conf_destroy(struct raft_instance *ri)
{
    if (ri->ri_csn_raft)
        ctl_svc_node_put(ri->ri_csn_raft);

    if (ri->ri_csn_this_peer)
        ctl_svc_node_put(ri->ri_csn_this_peer);

    for (int i = 0; i < CTL_SVC_MAX_RAFT_PEERS; i++)
        if (ri->ri_csn_raft_peers[i])
            ctl_svc_node_put(ri->ri_csn_raft_peers[i]);
}

/**
 * raft_server_instance_conf_init - Initialize this raft instance's config
 *    based on the 2 UUIDs passed in at startup time.  These UUIDs are for
 *    the Raft instance itself (and the peers involved) and the peer UUID for
 *    this instance.  The role of this function is to obtain the ctl_svc_node
 *    objects which pertain to these UUIDs so that basic config information
 *    can be obtained, such as: IP addresses, port numbers, and the raft log
 *    pathname.
 */
static int
raft_net_conf_init(struct raft_instance *ri)
{
    uuid_t tmp, tmp1;
    /* Check the ri for the needed the UUID strings.
     */
    if (!ri || !ri->ri_raft_uuid_str || !ri->ri_this_peer_uuid_str ||
        uuid_parse(ri->ri_this_peer_uuid_str, tmp) ||
        uuid_parse(ri->ri_raft_uuid_str, tmp1) ||
        !uuid_compare(tmp, tmp1))
        return -EINVAL;

    for (int i = RAFT_UDP_LISTEN_MIN; i < RAFT_UDP_LISTEN_MAX; i++)
        udp_socket_handle_init(&ri->ri_ush[i]);

    /* (re)initialize the ctl-svc node pointers.
     */
    ri->ri_csn_raft = NULL;
    ri->ri_csn_this_peer = NULL;
    for (int i = 0; i < CTL_SVC_MAX_RAFT_PEERS; i++)
        ri->ri_csn_raft_peers[i] = NULL;

    /* Lookup 'this' node's ctl-svc object.
     */
    int rc = ctl_svc_node_lookup_by_string(ri->ri_this_peer_uuid_str,
                                           &ri->ri_csn_this_peer);
    if (rc)
    {
        LOG_MSG(LL_ERROR,
                "ctl_svc_node_lookup() failed to find self UUID=%s\n"
                "Please check the local-control-service directory: %s",
                ri->ri_this_peer_uuid_str, ctl_svc_get_local_dir());

        goto cleanup;
    }

    /* Lookup the raft ctl-svc object.
     */
    rc = ctl_svc_node_lookup_by_string(ri->ri_raft_uuid_str, &ri->ri_csn_raft);
    if (rc)
    {
        LOG_MSG(LL_ERROR,
                "ctl_svc_node_lookup() failed to find raft UUID=%s\n"
                "Please check the local-control-service directory: %s",
                ri->ri_raft_uuid_str, ctl_svc_get_local_dir());

        goto cleanup;
    }

    DBG_CTL_SVC_NODE(LL_NOTIFY, ri->ri_csn_this_peer, "self");
    DBG_CTL_SVC_NODE(LL_NOTIFY, ri->ri_csn_raft, "raft");

    const struct ctl_svc_node_raft *csn_raft =
        ctl_svc_node_raft_2_raft(ri->ri_csn_raft);

    if (!csn_raft)
    {
        rc = -EINVAL;
        goto cleanup;
    }
    else if (csn_raft->csnr_num_members > CTL_SVC_MAX_RAFT_PEERS)
    {
        rc = -E2BIG;
        goto cleanup;
    }

    bool this_peer_found_in_raft_node = false;
    for (raft_peer_t i = 0; i < csn_raft->csnr_num_members; i++)
    {
        rc = ctl_svc_node_lookup(csn_raft->csnr_members[i].csrm_peer,
                                 &ri->ri_csn_raft_peers[i]);
        if (rc)
        {
            DECLARE_AND_INIT_UUID_STR(peer_uuid,
                                      csn_raft->csnr_members[i].csrm_peer);

            LOG_MSG(LL_ERROR,
                    "ctl_svc_node_lookup() failed to find raft-peer UUID=%s\n"
                    "Please check the local-control-service directory: %s",
                    peer_uuid, ctl_svc_get_local_dir());

            goto cleanup;
        }

        DECLARE_AND_INIT_UUID_STR(uuid_str,
                                  csn_raft->csnr_members[i].csrm_peer);

        DBG_CTL_SVC_NODE(LL_NOTIFY, ri->ri_csn_raft,
                         "raft-peer-%hhu %s", i, uuid_str);

        if (!ctl_svc_node_cmp(ri->ri_csn_this_peer, ri->ri_csn_raft_peers[i]))
            this_peer_found_in_raft_node = true;
    }

    if (!this_peer_found_in_raft_node && !raft_instance_is_client(ri))
    {
        rc = -ENODEV;
        goto cleanup;
    }

    return 0;

cleanup:
    raft_net_conf_destroy(ri);
    return rc;
}

int
raft_net_instance_shutdown(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int epoll_close_rc = raft_net_epoll_cleanup(ri);

    if (ri->ri_shutdown_cb)
        ri->ri_shutdown_cb(ri);

    int udp_sockets_close = raft_net_udp_sockets_close(ri);

    raft_net_timerfd_close(ri);

    raft_net_conf_destroy(ri);

    return (udp_sockets_close ? udp_sockets_close :
            (epoll_close_rc ? epoll_close_rc : 0));
}

static void
raft_net_histogram_setup(struct raft_instance *ri)
{
    for (enum raft_instance_hist_types i = RAFT_INSTANCE_HIST_MIN;
         i < RAFT_INSTANCE_HIST_MAX; i++)
    {
        binary_hist_init(&ri->ri_rihs[i].rihs_bh, 0,
                         RAFT_NET_BINARY_HIST_SIZE);

        ri->ri_rihs[i].rihs_type = i;
    }
}

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode)
{
    if (!ri)
        return -EINVAL;

    ri->ri_state = client_mode ? RAFT_STATE_CLIENT : RAFT_STATE_BOOTING;

    raft_net_histogram_setup(ri);

    int rc = raft_net_conf_init(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_conf_init(): %s", strerror(-rc));
        return rc;
    }

    rc = raft_net_udp_sockets_setup(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_udp_sockets_setup(): %s",
                       strerror(-rc));
        return rc;
    }

    rc = raft_net_timerfd_create(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_server_timerfd_create(): %s",
                       strerror(-rc));

        raft_net_instance_shutdown(ri);
        return rc;
    }

    rc = raft_net_epoll_setup(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_epoll_setup(): %s", strerror(-rc));

        raft_net_instance_shutdown(ri);
        return rc;
    }

    if (ri->ri_startup_pre_net_bind_cb)
    {
        rc = ri->ri_startup_pre_net_bind_cb(ri);
        if (rc)
        {
            SIMPLE_LOG_MSG(LL_WARN, "ri_startup_pre_net_bind_cb(): %s",
                           strerror(-rc));

            if (ri->ri_shutdown_cb)
                ri->ri_shutdown_cb(ri);

            return rc;
        }
    }

    /* bind() after adding the socket to the epoll set.
     */
    rc = raft_net_udp_sockets_bind(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_udp_sockets_bind(): %s",
                       strerror(-rc));

        if (ri->ri_shutdown_cb)
            ri->ri_shutdown_cb(ri);

        return rc;
    }

    return 0;
}

/**
 * raft_peer_2_idx - attempts to find the peer in the raft_instance
 *    "ri_csn_raft_peers" array.  If found, then the index of the peer is
 *    returned.  The returned index does not pertain to the raft configuration
 *    itself, as the raft config only works from a set of members which are
 *    not specifically labeled numerically.  The use of this function is to
 *    help track this candidate's vote tally.
 */
raft_peer_t
raft_peer_2_idx(const struct raft_instance *ri, const uuid_t peer_uuid)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft && ri->ri_csn_raft_peers);

    const raft_peer_t num_raft_peers =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    // Do not tolerate an invalid raft peers number
    NIOVA_ASSERT(num_raft_peers <= CTL_SVC_MAX_RAFT_PEERS);

    for (raft_peer_t i = 0; i < num_raft_peers; i++)
        if (!ctl_svc_node_compare_uuid(ri->ri_csn_raft_peers[i], peer_uuid))
            return i;

    return RAFT_PEER_ANY;
}

/**
 * raft_net_verify_sender_server_msg - verify that an incoming RPC's UUIDs
 *    match what it is expected based on the receiver's config.
 */
struct ctl_svc_node *
raft_net_verify_sender_server_msg(struct raft_instance *ri,
                                  const uuid_t sender_uuid,
                                  const uuid_t sender_raft_uuid,
                                  const struct sockaddr_in *sender_addr)
{
    if (!ri || !sender_uuid || uuid_is_null(sender_uuid))
        return NULL;

    /* Check the id of the sender to make sure they are part of the config
     * and that the RPC is for the correct raft instance.
     */
    const raft_peer_t sender_idx = raft_peer_2_idx(ri, sender_uuid);

    if (sender_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft) ||
        ctl_svc_node_compare_uuid(ri->ri_csn_raft, sender_raft_uuid))
    {
        DECLARE_AND_INIT_UUID_STR(raft_uuid, ri->ri_csn_raft->csn_uuid);
        DECLARE_AND_INIT_UUID_STR(peer_raft_uuid, sender_raft_uuid);

        LOG_MSG(
            LL_NOTIFY,
            "peer not found in my config %hhx %hhx, UUIDs (self=%s, peer=%s)",
            sender_idx, ctl_svc_node_raft_2_num_members(ri->ri_csn_raft),
            raft_uuid, peer_raft_uuid);

        return NULL;
    }

    struct ctl_svc_node *csn = ri->ri_csn_raft_peers[sender_idx];

    if (!ctl_svc_node_is_peer(csn))
        DBG_SIMPLE_CTL_SVC_NODE(LL_FATAL, csn, "csn is not a peer");

    const uint16_t expected_port = (raft_instance_is_client(ri) ?
                                    ctl_svc_node_peer_2_client_port(csn) :
                                    ctl_svc_node_peer_2_port(csn));

    if (ntohs(sender_addr->sin_port) != expected_port ||
        strncmp(ctl_svc_node_peer_2_ipaddr(csn),
                inet_ntoa(sender_addr->sin_addr), IPV4_STRLEN))
    {
        LOG_MSG(LL_NOTIFY, "uuid (%s) on unexpected IP:port (%s:%hu)",
                sender_uuid, inet_ntoa(sender_addr->sin_addr), expected_port);

        csn = NULL;
    }
    else if (!net_ctl_can_recv(&csn->csn_peer.csnp_net_ctl))
    {
        // Receive functionality is disabled in the ctl_svc layer.
        DBG_CTL_SVC_NODE(LL_DEBUG, csn, "net_ctl_can_recv() is false");
        csn = NULL;
    }

    return csn;
}

static int
raft_net_send_client_msgv_internal(struct raft_instance *ri,
                                   struct raft_client_rpc_msg *rcrm,
                                   const struct iovec *iov, size_t niovs)
{
    if (!ri || !ri->ri_csn_leader || !rcrm || !iov || !niovs)
        return -EINVAL;

    else if (niovs > 256)
        return -EMSGSIZE;

    const ssize_t msg_size =
        sizeof(struct raft_client_rpc_msg) + rcrm->rcrm_data_size;

    if (msg_size > RAFT_NET_MAX_RPC_SIZE)
        return -E2BIG;

    struct ctl_svc_node *csn = ri->ri_csn_leader;
    struct sockaddr_in dest;

    int rc = udp_setup_sockaddr_in(ctl_svc_node_peer_2_ipaddr(csn),
                                   ctl_svc_node_peer_2_client_port(csn),
                                   &dest);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "udp_setup_sockaddr_in(): %s (peer=%s:%hu)",
                strerror(-rc), ctl_svc_node_peer_2_ipaddr(csn),
                ctl_svc_node_peer_2_client_port(csn));

        return rc;
    }

    struct udp_socket_handle *ush = &ri->ri_ush[RAFT_UDP_LISTEN_CLIENT];

    ssize_t size_rc;
    if (!net_ctl_can_send(&csn->csn_peer.csnp_net_ctl))
    {
        DBG_CTL_SVC_NODE(LL_DEBUG, csn, "net_ctl_can_send() is false");
        size_rc = msg_size;
    }
    else
    {
        size_rc = udp_socket_send(ush, iov, niovs, &dest);
    }

    DBG_RAFT_CLIENT_RPC(LL_DEBUG, rcrm, &dest, "size-rc=%zd", size_rc);

    if (size_rc == msg_size)
        raft_net_update_last_comm_time(ri, csn->csn_uuid, true);

    return size_rc == msg_size ? 0 : -ECOMM;
}

int
raft_net_send_client_msg(struct raft_instance *ri,
                         struct raft_client_rpc_msg *rcrm)
{
    if (!ri || !ri->ri_csn_leader || !rcrm)
        return -EINVAL;

    struct iovec iov = {
        .iov_len = sizeof(struct raft_client_rpc_msg) + rcrm->rcrm_data_size,
        .iov_base = (void *)rcrm,
    };

    return raft_net_send_client_msgv_internal(ri, rcrm, &iov, 1);
}

int
raft_net_send_client_msgv(struct raft_instance *ri,
                          struct raft_client_rpc_msg *rcrm,
                          const struct iovec *iov, size_t niovs)
{
    if (!ri || !ri->ri_csn_leader || !rcrm)
        return -EINVAL;

    else if (niovs > 255 ||
             io_iovs_total_size_get(iov, niovs) != rcrm->rcrm_data_size)
        return -EMSGSIZE;

    struct iovec my_iovs[niovs + 1];
    my_iovs[0].iov_len = sizeof(struct raft_client_rpc_msg);
    my_iovs[0].iov_base = (void *)rcrm;

    // Copy the remaining IOVs into the local iov array
    memcpy(&my_iovs[1], iov, (sizeof(struct iovec) * niovs));

    return raft_net_send_client_msgv_internal(ri, rcrm, my_iovs, niovs + 1);
}

int
raft_net_verify_sender_client_msg(struct raft_instance *ri,
                                  const uuid_t sender_raft_uuid)
{
    if (!ri || uuid_is_null(sender_raft_uuid))
        return -EINVAL;

    return ctl_svc_node_compare_uuid(ri->ri_csn_raft, sender_raft_uuid) ?
        -ENODEV : 0;
}

/**
 * raft_net_update_last_comm_time - may be used by application level send and
 *     recv handlers to take and record a communication timestamp.
 * @ri:  raft instance pointer
 * @peer_uuid:  server peer uuid (client UUIDs should not be used here).
 * @send_or_recv:  non-zero for 'send'.
 */
void
raft_net_update_last_comm_time(struct raft_instance *ri,
                               const uuid_t peer_uuid, bool send_or_recv)
{
    if (!ri || uuid_is_null(peer_uuid))
        return;

    const raft_peer_t peer_idx = raft_peer_2_idx(ri, peer_uuid);

    if (peer_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft))
        return;

    struct timespec *ts = send_or_recv ?
        &ri->ri_last_send[peer_idx] : &ri->ri_last_recv[peer_idx];

    // ~1 ms granularity which should be fine for this app.
    niova_realtime_coarse_clock(ts);
}

int
raft_net_comm_get_last_recv(struct raft_instance *ri, const uuid_t peer_uuid,
                            struct timespec *ts)
{
    if (!ri || !ts || uuid_is_null(peer_uuid))
        return -EINVAL;

    const raft_peer_t peer_idx = raft_peer_2_idx(ri, peer_uuid);

    if (peer_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft))
	return -ERANGE;

    *ts = ri->ri_last_recv[peer_idx];

    return 0;
}

int
raft_net_comm_recency(const struct raft_instance *ri,
                      raft_peer_t raft_peer_idx,
                      enum raft_net_comm_recency_type type,
                      unsigned long long *ret_ms)
{
    if (!ri || !ri->ri_csn_raft || !ret_ms ||
        raft_peer_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft))
        return -EINVAL;

    const unsigned long long last_send =
        timespec_2_msec(&ri->ri_last_send[raft_peer_idx]);

    const unsigned long long last_recv =
        timespec_2_msec(&ri->ri_last_recv[raft_peer_idx]);

    unsigned long long now = niova_realtime_coarse_clock_get_msec();

    // This should not happen, but just in case..
    if (now < MAX(last_recv, last_send))
        now = MAX(last_recv, last_send);

    int rc = 0;

    switch (type)
    {
    case RAFT_COMM_RECENCY_RECV:
        *ret_ms = last_send ? (now - last_recv) : 0;
        break;
    case RAFT_COMM_RECENCY_SEND:
        *ret_ms = last_send ? (now - last_send) : 0;
        break;
    case RAFT_COMM_RECENCY_UNACKED_SEND:
        *ret_ms = (last_send > last_recv) ? (now - last_recv) : 0;
        break;
    default:
        rc = -EINVAL;
        break;
    }

    return rc;
}

raft_peer_t
raft_net_get_most_recently_responsive_server(const struct raft_instance *ri)
{
    const raft_peer_t nraft_servers =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    raft_peer_t start_peer = random_get() % nraft_servers;
    raft_peer_t best_peer = start_peer;

    unsigned long long recency_value = 0;

    for (raft_peer_t i = 0; i < nraft_servers; i++)
    {
        raft_peer_t idx = (i + start_peer) % nraft_servers;
        unsigned long long since_last_recv = 0;

        int rc = raft_net_comm_recency(ri, idx, RAFT_COMM_RECENCY_RECV,
                                       &since_last_recv);

        FATAL_IF((rc), "raft_net_comm_recency(): %s", strerror(-rc));

        if (since_last_recv < recency_value)
        {
            best_peer = idx;
            recency_value = since_last_recv;
        }
    }

    return best_peer;
}

static int
raft_net_server_target_check_by_idx(const struct raft_instance *ri,
                                    const raft_peer_t idx,
                                    const unsigned long long stale_timeout_ms)
{
    unsigned long long recency_ms = 0;

    int rc = raft_net_comm_recency(ri, idx, RAFT_COMM_RECENCY_UNACKED_SEND,
                                   &recency_ms);

    if (!rc)
        return (recency_ms > stale_timeout_ms) ? -ETIMEDOUT : 0;

    return rc;

}

int
raft_net_server_target_check(const struct raft_instance *ri,
                             const uuid_t server_uuid,
                             const unsigned long long stale_timeout_ms)
{
    return raft_net_server_target_check_by_idx(
        ri, raft_peer_2_idx(ri, server_uuid), stale_timeout_ms);
}

int
raft_net_apply_leader_redirect(struct raft_instance *ri,
                               const uuid_t redirect_target,
                               const unsigned long long stale_timeout_ms)
{
    if (!ri || uuid_is_null(redirect_target))
        return -EINVAL;

    raft_peer_t leader_idx = raft_peer_2_idx(ri, redirect_target);
    if (leader_idx == RAFT_PEER_ANY)
        return -ENOENT;

    // raft_peer_2_idx() should not return an out-of-bounds value.
    NIOVA_ASSERT(leader_idx < CTL_SVC_MAX_RAFT_PEERS);

    ri->ri_csn_leader = ri->ri_csn_raft_peers[leader_idx];

    int rc = raft_net_server_target_check_by_idx(ri, leader_idx,
                                                 stale_timeout_ms);

    if (rc == -ETIMEDOUT)
        timespec_clear(&ri->ri_last_send[leader_idx]); // "unstale" the leader

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "new leader via redirect (idx=%hhu)",
                      leader_idx);

    return 0;
}

void
raft_net_timerfd_settime(struct raft_instance *ri, unsigned long long msecs)
{
    struct itimerspec its = {0};

    msec_2_timespec(&its.it_value, msecs);

    int rc = timerfd_settime(ri->ri_timer_fd, 0, &its, NULL);

    FATAL_IF((rc), "timerfd_settime(): %s", strerror(errno));
}

raft_net_timerfd_cb_ctx_t
raft_net_timerfd_cb(const struct epoll_handle *eph)
{
    struct raft_instance *ri = eph->eph_arg;

    ssize_t rc = io_fd_drain(ri->ri_timer_fd, NULL);
    if (rc)
    {
        // Something went awry with the timerfd read.
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "io_fd_drain(): %zd", rc);
        return;
    }

    if (ri->ri_ignore_timerfd)
        raft_net_timerfd_settime(ri, 1);

    else if (ri->ri_timer_fd_cb)
        ri->ri_timer_fd_cb(ri);
}

static enum raft_udp_listen_sockets
raft_net_udp_identify_socket(const struct raft_instance *ri, const int fd)
{
    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX; i++)
        if (udp_socket_handle_2_sockfd(&ri->ri_ush[i]) == fd)
            return i;

    return RAFT_UDP_LISTEN_ANY;
}

/**
 * raft_net_udp_cb - this is the receive handler for all incoming UDP
 *    requests and replies.  The program is single threaded so the msg sink
 *    buffers are allocated statically here.  Operations that can be handled
 *    from this callback are:  client RPC requests, vote requests (if
 *    peer is candidate), vote replies (if self is candidate).
 */
static raft_net_udp_cb_ctx_t
raft_net_udp_cb(const struct epoll_handle *eph)
{
    static char sink_buf[RAFT_NET_MAX_RPC_SIZE];
    static struct sockaddr_in from;
    static struct iovec iovs[1] = {
        [0].iov_base = (void *)sink_buf,
        [0].iov_len  = RAFT_NET_MAX_RPC_SIZE,
    };

    NIOVA_ASSERT(eph && eph->eph_arg);

    struct raft_instance *ri = eph->eph_arg;
    NIOVA_ASSERT(ri);

    /* Clear the fd descriptor before doing any other error checks on the
     * sender.
     */
    ssize_t recv_bytes =
        udp_socket_recv_fd(eph->eph_fd, iovs, 1, &from, false);

    if (recv_bytes < 0) // return from a general recv error
    {
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "udp_socket_recv_fd():  %s",
                          strerror(-recv_bytes));
        return;
    }

    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "fd=%d type=%d rc=%zd",
                      eph->eph_fd,
                      raft_net_udp_identify_socket(ri, eph->eph_fd),
                      recv_bytes);

    switch (raft_net_udp_identify_socket(ri, eph->eph_fd))
    {
    case RAFT_UDP_LISTEN_SERVER:
        if (ri->ri_udp_server_recv_cb)
            ri->ri_udp_server_recv_cb(ri, sink_buf, recv_bytes, &from);
        break;
    case RAFT_UDP_LISTEN_CLIENT:
        if (ri->ri_udp_client_recv_cb)
            ri->ri_udp_client_recv_cb(ri, sink_buf, recv_bytes, &from);
        break;
    default:
        break;
    }
}

/**
 * raft_net_write_supp_get - looks up a supplment pointer base on the value
 *    of the provided handle.  Note, that a NULL handle is permitted.
 */
static struct raft_net_wr_supp *
raft_net_write_supp_get(struct raft_net_sm_write_supplements *rnsws,
                        void *handle)
{
    NIOVA_ASSERT(rnsws->rnsws_nitems < RAFT_NET_WR_SUPP_MAX);

    for (size_t i = 0; i < rnsws->rnsws_nitems; i++)
        if (handle == rnsws->rnsws_ws[i].rnws_handle)
            return &rnsws->rnsws_ws[i];

    if (rnsws->rnsws_nitems == RAFT_NET_WR_SUPP_MAX)
        return NULL;

    int rc = niova_reallocarray(rnsws->rnsws_ws, struct raft_net_wr_supp,
                                rnsws->rnsws_nitems + 1UL);
    if (rc)
        return NULL;

    rnsws->rnsws_nitems++;

    // Initialize pointers to NULL
    memset(&rnsws->rnsws_ws[rnsws->rnsws_nitems], 0,
           sizeof(struct raft_net_wr_supp));

    rnsws->rnsws_ws[rnsws->rnsws_nitems].rnws_handle = handle;

    return &rnsws->rnsws_ws[rnsws->rnsws_nitems];
}

static void
raft_net_write_supp_destroy(struct raft_net_wr_supp *ws)
{
    if (!ws || !ws->rnws_nkv)
        return;

    for (size_t i = 0; i < ws->rnws_nkv; i++)
    {
        niova_free(ws->rnws_keys[i]);
        niova_free(ws->rnws_values[i]);
    }

    niova_free(ws->rnws_keys);
    niova_free(ws->rnws_values);
    niova_free(ws->rnws_key_sizes);
    niova_free(ws->rnws_value_sizes);

    if (ws->rnws_comp_cb)
        ws->rnws_comp_cb(ws->rnws_handle);
}

static int
raft_net_write_supp_add(struct raft_net_wr_supp *ws, const char *key,
                        const size_t key_size, const char *value,
                        const size_t value_size)
{
    if (!ws || !key || !key_size)
        return -EINVAL;

    else if (ws->rnws_nkv == RAFT_NET_WR_SUPP_MAX)
        return -ENOSPC;

    NIOVA_ASSERT(ws->rnws_nkv < RAFT_NET_WR_SUPP_MAX);

    size_t n = ws->rnws_nkv;

    int rc = niova_reallocarray(ws->rnws_keys, char *, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(ws->rnws_key_sizes, size_t, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(ws->rnws_values, char *, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(ws->rnws_value_sizes, size_t, n + 1UL);
    if (rc)
        return rc;

    ws->rnws_keys[n] = niova_malloc(key_size);
    if (!ws->rnws_keys[n])
        return -ENOMEM;

    ws->rnws_values[n] = niova_malloc(value_size);
    if (!ws->rnws_values[n])
    {
        niova_free(ws->rnws_keys[n]);
        return -ENOMEM;
    }

    memcpy(ws->rnws_keys[n], key, key_size);
    memcpy(ws->rnws_values[n], value, value_size);

    ws->rnws_key_sizes[n] = key_size;
    ws->rnws_value_sizes[n] = value_size;

    ws->rnws_nkv++;

    LOG_MSG(LL_DEBUG, "ws=%p nkv=%zu key=%s val=%p", ws, ws->rnws_nkv, key,
            value);

    return 0;
}

int
raft_net_client_user_id_parse(const char *in,
                              struct raft_net_client_user_id *rncui,
                              const version_t version)
{
    if (!in || !rncui)
        return -EINVAL;

    else if (version != 0)
        return -EOPNOTSUPP;

    // An otherwise invalid string could appear valid after the below strncpy
    else if (strnlen(in, RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE) ==
        RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE)
        return -ENAMETOOLONG;

    char local_str[RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE];
    strncpy(local_str, in, RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE - 1);
    local_str[RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE - 1] = '\0';

    const char *uuid_str = NULL;

    int rc = regexec(&raftNetRncuiRegex, local_str, 0, NULL, 0);
    if (!rc)
    {
        const char *sep = RAFT_NET_CLIENT_USER_ID_V0_STR_SEP;
        char *sp = NULL;
        char *sub;

        size_t pos = 0;
        for (sub = strtok_r(local_str, sep, &sp);
             sub != NULL;
             sub = strtok_r(NULL, sep, &sp), pos++)
        {
            if (!pos)
            {
                rc = uuid_parse(sub, RAFT_NET_CLIENT_USER_ID_2_UUID(rncui, 0,
                                                                    0));
                if (rc)
                {
                    rc = -EINVAL;
                    break;
                }

                uuid_str = sub;
            }
            else
            {
                NIOVA_ASSERT((1 + pos) < RAFT_NET_CLIENT_USER_ID_V0_NUINT64);

                RAFT_NET_CLIENT_USER_ID_2_UINT64(rncui, 0, 1 + pos) =
                    strtoull(sub, NULL, 16);
            }
        }
    }

    if (!rc)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, RAFT_NET_CLIENT_USER_ID_FMT,
                       RAFT_NET_CLIENT_USER_ID_FMT_ARGS(rncui, uuid_str, 0));
    }
    else
    {
        LOG_MSG(LL_NOTIFY, "parse failed for `%s'", local_str);
    }

    return rc;
}

/**
 * raft_net_sm_write_supplement_add - 'write supplements' are KVs which
 *    accompany a backend write operation and are atomically applied to the
 *    backend with that write operation.  There are 2 cases where supplements
 *    are used.  First, is the writing of a raft-log-entry, here, the state-
 *    machine (SM) may require that certain KVs are emplaced at the time of the
 *    log entry write.  The second case occurs when a raft instance calls out
 *    to the SM to apply a committed raft-log-entry.  Here raft itself may wish
 *    to update its state reflecting the SM apply operation.
 */
int
raft_net_sm_write_supplement_add(
    struct raft_net_sm_write_supplements *rnsws, void *handle,
    void (*rnws_comp_cb)(void *),
    const char *key, const size_t key_size, const char *value,
    const size_t value_size)
{
    if (!rnsws || !key || !key_size)
        return -EINVAL;

    struct raft_net_wr_supp *ws = raft_net_write_supp_get(rnsws, handle);
    if (!ws)
        return -ENOMEM;

    if (rnws_comp_cb) // Apply the callback if it was specified
        ws->rnws_comp_cb = rnws_comp_cb;

    return raft_net_write_supp_add(ws, key, key_size, value, value_size);
}

void
raft_net_sm_write_supplement_destroy(
    struct raft_net_sm_write_supplements *rnsws)
{
    if (!rnsws || !rnsws->rnsws_nitems)
        return;

    for (size_t i = 0; i < rnsws->rnsws_nitems; i++)
        raft_net_write_supp_destroy(&rnsws->rnsws_ws[i]);

    niova_free(rnsws->rnsws_ws);

    rnsws->rnsws_ws = NULL;
}

void
raft_net_sm_write_supplement_init(struct raft_net_sm_write_supplements *rnsws)
{
    if (rnsws)
        rnsws->rnsws_nitems = 0;
}

void
raft_net_instance_apply_callbacks(struct raft_instance *ri,
                                  raft_net_timer_cb_t timer_fd_cb,
                                  raft_net_udp_cb_t udp_client_recv_cb,
                                  raft_net_udp_cb_t udp_server_recv_cb)
{
    NIOVA_ASSERT(ri);

    ri->ri_timer_fd_cb = timer_fd_cb;
    ri->ri_udp_client_recv_cb = udp_client_recv_cb;
    ri->ri_udp_server_recv_cb = udp_server_recv_cb;
}

static init_ctx_t NIOVA_CONSTRUCTOR(RAFT_SYS_CTOR_PRIORITY)
raft_net_init(void)
{
    FUNC_ENTRY(LL_NOTIFY);
    LREG_ROOT_OBJECT_ENTRY_INSTALL(raft_net_info);

    int rc = regcomp(&raftNetRncuiRegex, RNCUI_V0_REGEX_BASE, 0);
    NIOVA_ASSERT(!rc);

    return;
}

static destroy_ctx_t NIOVA_DESTRUCTOR(RAFT_SYS_CTOR_PRIORITY)
raft_net_destroy(void)
{
    regfree(&raftNetRncuiRegex);
}
