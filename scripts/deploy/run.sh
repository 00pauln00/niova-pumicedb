#!/bin/bash
# pumice.sh
# Start the Pumice node based on container IP using a single grep loop
export NIOVA_LOCAL_CTL_SVC_DIR="/work/pumice/configs"
export LD_LIBRARY_PATH="/lib:/work/pumice/lib"
export NIOVA_INOTIFY_BASE_PATH="/work/pumice/ctl"

# Detect container IP inside the Docker network
MY_IP=$(hostname -i | awk '{print $1}')
echo "Container IP detected as $MY_IP"

# Get Raft UUID (assume only one .raft file exists)
RAFT_FILE=$(ls -1 /work/pumice/configs/*.raft | head -n1)
RAFT_UUID=$(basename "$RAFT_FILE" .raft)

if [ -z "$RAFT_UUID" ]; then
    echo "ERROR: No .raft file found in configs/"
    exit 1
fi

# Find the .peer file containing the matching IPADDR in one grep pipeline
PEER_FILE=$(grep -il "IPADDR[[:space:]]\+$MY_IP" /work/pumice/configs/*.peer | head -n1)

if [ -z "$PEER_FILE" ]; then
    echo "ERROR: No .peer file found with IPADDR=$MY_IP"
    exit 1
fi

PEER_UUID=$(basename "$PEER_FILE" .peer)
echo "Starting node with PEER_UUID=$PEER_UUID and RAFT_UUID=$RAFT_UUID"

# Start pmdb server
/work/pumice/libexec/niova/pumice-reference-server \
    -r "$RAFT_UUID" \
    -u "$PEER_UUID" \
    -a \
    -c > "/work/pumice/logs/pmdb_server_${PEER_UUID}_stdouterr" 2>&1 &

sleep 5

# Start proxy client
CUUID=$(uuidgen)
/work/pumice/libexec/niova/pumice-reference-client \
    -r "$RAFT_UUID" \
    -u "$CUUID" \
    -a > "/work/pumice/logs/pmdb_client_${CUUID}_stdouterr" 2>&1
