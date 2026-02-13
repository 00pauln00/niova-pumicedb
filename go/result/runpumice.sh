#!/bin/bash
killall pumice-reference-server
killall pumice-reference-client
rm -rf files
cp -r setup files

# Env setup
export LD_LIBRARY_PATH=/home/sshivkumar/niovacorelibs/lib/
export NIOVA_THREAD_COUNT=2
export NIOVA_INOTIFY_BASE_PATH=/home/sshivkumar/niova-pumicedb/go/result/files/ctl-interface
export NIOVA_LOCAL_CTL_SVC_DIR=/home/sshivkumar/niova-pumicedb/go/result/files/configs

# Extract raft UUID (only one .raft file expected)
RAFT_UUID=$(ls $NIOVA_LOCAL_CTL_SVC_DIR/*.raft | xargs -n1 basename | cut -d. -f1)

# Extract peer UUIDs (sorted for stable assignment)
PEER_UUIDS=($(ls $NIOVA_LOCAL_CTL_SVC_DIR/*.peer | xargs -n1 basename | cut -d. -f1 | sort))

# Start servers with -c flag, mapping cores sequentially
i=0
for PEER in "${PEER_UUIDS[@]}"; do
    CORE1=$((i * 2))
    CORE2=$((i * 2 + 1))
    LOGFILE="server$((i+1))"
    echo "Starting server$((i+1)) on cores $CORE1,$CORE2 with -r $RAFT_UUID -u $PEER"
    taskset -c $CORE1,$CORE2 ../../test/pumice-reference-server -c -r $RAFT_UUID -u $PEER -a > "$LOGFILE" 2>&1 &
    ((i++))
done

#Wait for server to start and leader to be elected
sleep 5

cuuid=$(uuidgen)
echo $cuuid

# Run pumice client
taskset -c 10,11,12,13 \
../../test/pumice-reference-client \
  -r ${RAFT_UUID} \
  -u ${cuuid} \
  -a > client 2>&1 &

sleep 5

if [ $# -ne 1 ]; then
  echo "Usage: $0 <n>"
  exit 1
fi

n=$1

for i in $(seq 1 $n); do
  app=$(uuidgen)
  filename="op$i.txt"
  cat <<EOF > "$NIOVA_INOTIFY_BASE_PATH/$cuuid/input/$filename"
APPLY input@${app}:0:0:0:0.write:0.10000
WHERE /pumice_db_test_client/input
OUTFILE /op$i
EOF
  echo "Created $filename"
done