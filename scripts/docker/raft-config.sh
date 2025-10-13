#!/bin/bash
mkdir -p configs
cd configs

if [ -z "$1" ]; then
    echo "Usage: $0 <starting-port>"
    exit 1
fi

START_PORT=$1
CLIENT_PORT=$((START_PORT + 2000))  # offset client port arbitrarily

# Generate RAFT UUID
RAFT_UUID=$(uuidgen)
RAFT_FILE="${RAFT_UUID}.raft"
echo "RAFT ${RAFT_UUID}" > "$RAFT_FILE"

# Generate and process 5 PEER UUIDs
for i in {0..4}; do
    PEER_UUID=$(uuidgen)
    echo "PEER ${PEER_UUID}" >> "$RAFT_FILE"

    PEER_PORT=$((START_PORT + i))
    PEER_CLIENT_PORT=$((CLIENT_PORT + i))
    PEER_FILE="${PEER_UUID}.peer"

    cat <<EOF > "$PEER_FILE"
RAFT         ${RAFT_UUID}
IPADDR       127.0.0.1
PORT         ${PEER_PORT}
CLIENT_PORT  ${PEER_CLIENT_PORT}
STORE        /home/pumice/${PEER_UUID}.raftdb
EOF

    echo "Generated config: $PEER_FILE"
done

echo "Generated RAFT file: $RAFT_FILE"

cd ..
