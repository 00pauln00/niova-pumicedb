#!/bin/bash
mkdir -p configs
cd configs


# Generate RAFT UUID
START_IP_BASE="172.18.0."
PEER_PORT=7000
CLIENT_PORT=8000
RAFT_UUID=$(uuidgen)
RAFT_FILE="${RAFT_UUID}.raft"
echo "RAFT ${RAFT_UUID}" > "$RAFT_FILE"


for i in {0..4}; do
    PEER_UUID=$(uuidgen)
    echo "PEER ${PEER_UUID}" >> "$RAFT_FILE"
    PEER_FILE="${PEER_UUID}.peer"

    # Assign static IP based on Docker Compose network
    PEER_IP="${START_IP_BASE}$((i + 2))"  # 172.18.0.2 → node0, etc.

    cat <<EOF > "$PEER_FILE"
RAFT         ${RAFT_UUID}
IPADDR       ${PEER_IP}
PORT         ${PEER_PORT}
CLIENT_PORT  ${CLIENT_PORT}
STORE        /home/pumice/${PEER_UUID}.raftdb
EOF

    echo "Generated $PEER_FILE for $PEER_IP"
done

echo "Generated RAFT file: $RAFT_FILE"

cd ..
