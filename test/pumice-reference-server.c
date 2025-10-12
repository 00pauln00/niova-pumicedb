/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <unistd.h>
#include <uuid/uuid.h>

#include <rocksdb/c.h>

#include "niova/niova_backtrace.h"

#include "niova/common.h"
#include "niova/registry.h"
#include "raft/raft_net.h"
#include "raft/raft_test.h"

#include "pumice_db.h"

#define OPTS "u:r:hac"

REGISTRY_ENTRY_FILE_GENERATE;

static const char *pmdbts_column_family_name = "PMDBTS_CF";
static bool syncPMDBWrites = true;
static bool coalescedWrites = false;
const char *raft_uuid_str;
const char *my_uuid_str;

#define MAX_KEYS 100
#define KEY_PREFIX "key"

#define PMDTS_ENTRY_KEY_LEN sizeof(struct raft_net_client_user_key_v0)
#define PMDTS_RNCUI_2_KEY(rncui) (const char *)&(rncui)->rncui_key.v0

static rocksdb_column_family_handle_t *
pmdbst_get_cfh(void)
{
    /* Currently the lookup is always performed (as opposed to returning a
     * cached cf pointer).  This is because the cf handles will change if
     * the underlying raft instance has underwent recovery.
     */
    rocksdb_column_family_handle_t *pmdbts_cfh =
        PmdbCfHandleLookup(pmdbts_column_family_name);

    NIOVA_ASSERT(pmdbts_cfh);

    return pmdbts_cfh;
}

// Initialize keys 1-100 with value 0
static void
pmdbts_init_keys(struct pumicedb_cb_cargs *args)
{
    (void)args; // Suppress unused parameter warning
    
    // Note: This function is called during initialization, so we need to use
    // direct RocksDB operations since PmdbWriteKV requires a pmdb_handle
    // which may not be available during init phase.
    rocksdb_column_family_handle_t *cfh = pmdbst_get_cfh();
    char *err = NULL;
    
    // Initialize keys 1-100 with value 0
    for (int i = 1; i <= MAX_KEYS; i++) {
        char key_name[32];
        uint64_t value = 0;
        
        snprintf(key_name, sizeof(key_name), "%s%d", KEY_PREFIX, i);
        
        // Write key with value 0 using direct RocksDB operations
        rocksdb_writeoptions_t *write_opts = rocksdb_writeoptions_create();
        rocksdb_put_cf(PmdbGetRocksDB(), write_opts, cfh,
                       key_name, strlen(key_name),
                       (const char*)&value, sizeof(value),
                       &err);
        
        rocksdb_writeoptions_destroy(write_opts);
        
        if (err) {
            SIMPLE_LOG_MSG(LL_ERROR, "Failed to initialize key %s: %s", key_name, err);
            // Just log the error and continue - don't return from void function
        }
    }
    
    SIMPLE_LOG_MSG(LL_NOTIFY, "Initialized %d keys with value 0", MAX_KEYS);
}

// Write preparation function - generates random number
static pumicedb_write_prep_ctx_ssize_t
pmdbts_write_prep(struct pumicedb_cb_cargs *args)
{
    const void *input_buf = args->pcb_req_buf;
    size_t input_bufsz = args->pcb_req_bufsz;
    
    // Check if this is a logical command using the new flag
    if (input_bufsz >= sizeof(struct raft_test_data_block)) {
        const struct raft_test_data_block *rtdb = 
            (const struct raft_test_data_block *)input_buf;
            
        if (rtdb->rtdb_logical_cmd == true) {
            // Generate random values for logical command
            uint32_t random_count = (rand() % MAX_KEYS) + 1;
            uint32_t increment_value = (rand() % 1000) + 1;
            
            // Store the random values in the input buffer for apply to use
            struct raft_test_data_block *input_rtdb = (struct raft_test_data_block *)input_buf;
            input_rtdb->rtdb_random_count = random_count;
            input_rtdb->rtdb_increment_value = increment_value;
            
            SIMPLE_LOG_MSG(LL_WARN, "LOGICAL_CMD: Generated random_count=%u, increment_value=%u", 
                          random_count, increment_value);
            
            // Return 0 to continue with normal processing
            return 0;
        }
    }
    
    // Not a logical command, return 0 to continue with normal processing
    return 0;
}

#if 0
static int
pmdbst_init_rocksdb(void)
{
    if (pmdbts_cfh)
        return 0;

    rocksdb_options_t *opts = rocksdb_options_create();
    if (!opts)
        return -ENOMEM;

    char *err = NULL;
    rocksdb_options_set_create_if_missing(opts, 1);

    pmdbts_cfh = rocksdb_create_column_family(PmdbGetRocksDB(), opts,
                                              pmdbts_column_family_name, &err);

    rocksdb_options_destroy(opts);

    if (err)
    {
        pmdbts_cfh = NULL;
        SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_create_column_family(): %s",
                       err);
    }

    return err ? -EINVAL : 0;;
}
#endif

static int
pmdbts_lookup(const struct raft_net_client_user_id *app_id,
              struct raft_test_values *rtv)
{
    if (!app_id || !rtv)
        return -EINVAL;

    char *err = NULL;

    rocksdb_readoptions_t *ropts = rocksdb_readoptions_create();
    if (!ropts)
        return -ENOMEM;

    size_t value_len = 0;

    char *value = rocksdb_get_cf(PmdbGetRocksDB(), ropts, pmdbst_get_cfh(),
                                 PMDTS_RNCUI_2_KEY(app_id),
                                 PMDTS_ENTRY_KEY_LEN, &value_len, &err);

    rocksdb_readoptions_destroy(ropts);

    int rc = 0;

    if (!value) // Xxx need better error interpretation
    {
        rc = -ENOENT;
    }
    else if (err)
    {
        DECLARE_AND_INIT_UUID_STR(key, app_id->rncui_key.v0.rncui_v0_uuid[0]);
        SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_get_cf(`%s.%lx.%lx`): %s",
                       key, app_id->rncui_key.v0.rncui_v0_uint64[2],
                       app_id->rncui_key.v0.rncui_v0_uint64[3], err);
        rc = -EINVAL;
    }
    else if (value_len != sizeof(struct raft_test_values))
    {
        rc = -EBADMSG;
    }
    else
    {
        *rtv = *(struct raft_test_values *)value;
    }

    if (value)
        free(value);

    return rc;
}

static int
pmdbts_apply_lookup_and_check(const struct raft_net_client_user_id *app_id,
                              const char *input_buf, size_t input_bufsz,
                              struct raft_test_values *ret_rtv)
{
    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)input_buf;

    if (!rtdb || (ssize_t)input_bufsz != raft_test_data_block_total_size(rtdb))
    {
        SIMPLE_LOG_MSG(LL_ERROR,
                       "null input buf or invalid size (%zu) "
                       "raft_test_data_block_total_size(): %zd",
                       input_bufsz, raft_test_data_block_total_size(rtdb));

        return -EINVAL;
    }
    else if (uuid_compare(RAFT_NET_CLIENT_USER_ID_2_UUID(app_id, 0, 0),
                          rtdb->rtdb_client_uuid))
    {
        DECLARE_AND_INIT_UUID_STR(
            app_uuid_str, RAFT_NET_CLIENT_USER_ID_2_UUID(app_id, 0, 0));

        DBG_RAFT_TEST_DATA_BLOCK(LL_ERROR, rtdb, "mismatched UUID=%s",
                                 app_uuid_str);
        return -EBADMSG;
    }
    else if (rtdb->rtdb_num_values == 0)
    {
        DBG_RAFT_TEST_DATA_BLOCK(LL_ERROR, rtdb, "num_values == 0");
        return -ENOMSG;
    }

    // Lookup the app uuid in the DB.
    struct raft_test_values current_rtv;
    int rc = pmdbts_lookup(app_id, &current_rtv);

    if (rc == -ENOENT)
    {
        memset(&current_rtv, 0, sizeof(struct raft_test_values));
        DBG_RAFT_TEST_DATA_BLOCK(LL_NOTIFY, rtdb, "entry does not yet exist");
    }
    else if (rc)
    {
        DBG_RAFT_TEST_DATA_BLOCK(LL_ERROR, rtdb, "pmdbts_lookup(): %s",
                                 strerror(-rc));
        // There was some other problem with the DB read - return here
        return rc;
    }

    DBG_RAFT_TEST_DATA_BLOCK(LL_DEBUG, rtdb,
                             "pmdbts_lookup(): current seqno=%ld, val=%ld",
                             current_rtv.rtv_seqno,
                             current_rtv.rtv_reply_xor_all_values);

    // Check the sequence is correct and the contents are valid.
    for (uint16_t i = 0; i < rtdb->rtdb_num_values; i++)
    {
        const struct raft_test_values *prov_rtv = &rtdb->rtdb_values[i];

        if ((current_rtv.rtv_seqno + i + 1) != prov_rtv->rtv_seqno)
        {
            DBG_RAFT_TEST_DATA_BLOCK(
                LL_ERROR, rtdb, "invalid sequence @idx-%hu(%ld) (current=%ld)",
                i, prov_rtv->rtv_seqno, current_rtv.rtv_seqno);

            return -EILSEQ;
        }
    }

    // Success
    *ret_rtv = current_rtv;

    return 0;
}

static void
pmdbts_sum_incoming_rtv(const struct raft_test_data_block *rtdb_src,
                        struct raft_test_values *dest_rtv)
{
    for (uint16_t i = 0; i < rtdb_src->rtdb_num_values; i++)
    {
        const struct raft_test_values *src_rtv = &rtdb_src->rtdb_values[i];

        // These sequences should have already been checked!
        NIOVA_ASSERT((dest_rtv->rtv_seqno + 1) == src_rtv->rtv_seqno);

        dest_rtv->rtv_reply_xor_all_values ^= src_rtv->rtv_request_value;
        dest_rtv->rtv_seqno++;
    }
}

static pumicedb_apply_ctx_ssize_t
pmdbts_apply(struct pumicedb_cb_cargs *args)
{
//    NIOVA_ASSERT(!pmdbst_init_rocksdb());

    const struct raft_net_client_user_id *app_id = args->pcb_userid;
    const void *input_buf = args->pcb_req_buf;
    size_t input_bufsz = args->pcb_req_bufsz;
    void *pmdb_handle = args->pcb_pmdb_handler;

    // Check if this is a logical command using the new flag
    if (input_bufsz >= sizeof(struct raft_test_data_block)) {
        const struct raft_test_data_block *rtdb = 
            (const struct raft_test_data_block *)input_buf;
            
        if (rtdb->rtdb_logical_cmd == true) {
            // Get the random values that were set in write_prep
            uint32_t random_count = rtdb->rtdb_random_count;
            uint32_t increment_value = rtdb->rtdb_increment_value;
            
            if (random_count > MAX_KEYS) {
                random_count = MAX_KEYS;
            }
            
            SIMPLE_LOG_MSG(LL_WARN, "LOGICAL_CMD: Processing %u keys with increment_value=%u", 
                          random_count, increment_value);
            
            // Read and update the first 'random_count' keys
            for (uint32_t i = 1; i <= random_count; i++) {
                char key_name[32];
                uint64_t current_value = 0;
                uint64_t new_value = 0;
                size_t value_len = 0;
                
                snprintf(key_name, sizeof(key_name), "%s%u", KEY_PREFIX, i);
                
                // Read current value using direct RocksDB operations
                rocksdb_readoptions_t *read_opts = rocksdb_readoptions_create();
                char *err = NULL;
                char *get_value = rocksdb_get_cf(PmdbGetRocksDB(), read_opts, 
                                               pmdbst_get_cfh(),
                                               key_name, strlen(key_name), 
                                               &value_len, &err);
                rocksdb_readoptions_destroy(read_opts);
                
                if (get_value && !err && value_len == sizeof(uint64_t)) {
                    current_value = *(uint64_t*)get_value;
                    free(get_value);
                } else if (err) {
                    SIMPLE_LOG_MSG(LL_ERROR, "Failed to read key %s: %s", key_name, err);
                    continue;
                }
                
                // Calculate new value (current + increment_value)
                new_value = current_value + increment_value;
                
                // Write new value using PmdbWriteKV
                int rc = PmdbWriteKV(app_id, pmdb_handle, key_name, 
                                   strlen(key_name),
                                   (const char*)&new_value, sizeof(new_value), 
                                   NULL, (void *)pmdbst_get_cfh());
                
                if (rc) {
                    SIMPLE_LOG_MSG(LL_ERROR, "Failed to write key %s: rc=%d", key_name, rc);
                }
            }
            
            SIMPLE_LOG_MSG(LL_WARN, "LOGICAL_CMD: Completed processing %u keys with increment_value=%u", 
                          random_count, increment_value);
        }
    }

    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)input_buf;

    struct raft_test_values stored_rtv;
    int rc = pmdbts_apply_lookup_and_check(app_id, input_buf, input_bufsz,
                                           &stored_rtv);
    if (rc)
        return rc;

    pmdbts_sum_incoming_rtv(rtdb, &stored_rtv);

    DBG_RAFT_TEST_DATA_BLOCK(LL_DEBUG, rtdb, "new seqno=%ld, val=%ld",
                             stored_rtv.rtv_seqno,
                             stored_rtv.rtv_reply_xor_all_values);

    // Stage the KV back through pumiceDB.
    PmdbWriteKV(app_id, pmdb_handle, PMDTS_RNCUI_2_KEY(app_id),
                PMDTS_ENTRY_KEY_LEN, (const char *)&stored_rtv,
                sizeof(struct raft_test_values), NULL,
                (void *)pmdbst_get_cfh());

    return 0;
}

static pumicedb_read_ctx_ssize_t
pmdbts_read(struct pumicedb_cb_cargs *args)
{

    const struct raft_net_client_user_id *app_id =  args->pcb_userid;
    char *reply_buf = args->pcb_reply_buf;
    size_t reply_bufsz = args->pcb_reply_bufsz;

    if (!reply_buf || !reply_bufsz)
        return (ssize_t)-EINVAL;

#if 0 /* This code does not require a raft_test_data_block structure in the
       * request.
       */
    const struct raft_test_data_block *req_rtdb =
        (const struct raft_test_data_block *)request_buf;
    const ssize_t rrc = raft_test_data_block_total_size(req_rtdb);
    if (rrc != (ssize_t)request_bufsz)
    {
        DBG_RAFT_TEST_DATA_BLOCK(
            LL_NOTIFY, req_rtdb,
            "raft_test_data_block_total_size()=%zd != request_bufsz=%zu",
            rrc, request_bufsz);

        return rrc < 0 ? rrc : -EBADMSG;
    }
#endif

    if (reply_bufsz <
        (sizeof(struct raft_test_data_block) +
         sizeof(struct raft_test_values)))
        return -ENOSPC;

    struct raft_test_data_block *reply_rtdb =
        (struct raft_test_data_block *)reply_buf;

    uuid_copy(reply_rtdb->rtdb_client_uuid,
              RAFT_NET_CLIENT_USER_ID_2_UUID(app_id, 0, 0));

    reply_rtdb->rtdb_op = RAFT_TEST_DATA_OP_READ;

    int rc = pmdbts_lookup(app_id, &reply_rtdb->rtdb_values[0]);

    if (rc == -ENOENT)
        reply_rtdb->rtdb_num_values = 0;

    else if (!rc)
        reply_rtdb->rtdb_num_values = 1;

    else
        return (ssize_t)rc;

    return raft_test_data_block_total_size(reply_rtdb);
}

static void
pmdbts_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s -r <UUID> -u <UUID> [-c (coalesce-raft-writes)] [-a (async-raft-writes)]\n", argv[0]);

    exit(error);
}

static void
pmdbts_getopt(int argc, char **argv)
{
    if (!argc || !argv)
        return;

    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 'a':
            syncPMDBWrites = false;
            break;
        case 'c':
            coalescedWrites = true;
            break;
        case 'r':
            raft_uuid_str = optarg;
            break;
        case 'u':
            my_uuid_str = optarg;
            break;
        case 'h':
            pmdbts_print_help(0, argv);
            break;
        default:
            pmdbts_print_help(EINVAL, argv);
            break;
        }
    }

    if (!raft_uuid_str || !my_uuid_str)
        pmdbts_print_help(EINVAL, argv);
}

int
main(int argc, char **argv)
{
    pmdbts_getopt(argc, argv);

    struct PmdbAPI api = {
        .pmdb_write_prep   = pmdbts_write_prep,
        .pmdb_init         = pmdbts_init_keys,
        .pmdb_apply        = pmdbts_apply,
        .pmdb_read         = pmdbts_read,
        .pmdb_fill_reply   = NULL,
    };

    const char *cf_names[1] = {pmdbts_column_family_name};

    return PmdbExec(raft_uuid_str, my_uuid_str, &api, cf_names, 1,
                    syncPMDBWrites, coalescedWrites, NULL);
}
