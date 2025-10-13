#!/bin/bash
# pumice.sh
# $1 = container index (0,1,2,...)

INDEX=$1

if [ -z "$INDEX" ]; then
    echo "Usage: $0 <container-index>"
    exit 1
fi

mkdir -p logs

# Get Raft UUID (same for all nodes)
RAFT_FILE=$(ls -1 configs/*.raft | head -n1)
RAFT_UUID=$(basename "$RAFT_FILE" .raft)


if [ -z "$RAFT_UUID" ]; then
    echo "ERROR: No .raft file found in configs/"
    exit 1
fi

# Pick peer UUID based on alphabetical order and container index
PEER_FILE=$(ls -1 configs/*.peer | sed -n "$((INDEX+1))p")

if [ -z "$PEER_FILE" ]; then
    echo "ERROR: No .peer file found for index $INDEX"
    exit 1
fi

PEER_UUID=$(basename "$PEER_FILE" .peer)
echo "Starting node with PEER_UUID=$PEER_UUID and RAFT_UUID=$RAFT_UUID"

# Start pmdb server
./libexec/niova/pumice-reference-server \
    -r "$RAFT_UUID" \
    -u "$PEER_UUID" \
    -a \
    -c > "/pumice/logs/pmdb_server_${PEER_UUID}_stdouterr" 2>&1 &

sleep 5

# Start proxy client
CUUID=$(uuidgen)
./libexec/niova/pumice-reference-client \
    -r "$RAFT_UUID" \
    -u "$CUUID" \
    -a > "/pumice/logs/pmdb_client_${CUUID}_stdouterr" 2>&1