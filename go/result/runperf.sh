#!/bin/bash
killall perfserver
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
    taskset -c $CORE1,$CORE2 ../libexec/perfserver -c -a -r $RAFT_UUID -u $PEER > "$LOGFILE" 2>&1 &
    ((i++))
done

#Wait for server to start and leader to be elected
sleep 5

# Run perfclient for all scenarios
sizes=(256)
qds=(100)

for s in "${sizes[@]}"; do
  for q in "${qds[@]}"; do
    CLIENT_UUID=$(uuidgen)
    name="ryow_result_C_KV${s}_QD${q}_W"
    echo ">>> Running perfclient with size=${s}, QD=${q}, name=${name}"
    taskset -c 10,11,12,13 \
    ../libexec/perfclient \
      -r $RAFT_UUID \
      -u $CLIENT_UUID \
      -t 60 \
      -s "${s}" \
      -q "${q}" \
      -w "w" \
      -n "${name}"
  done
done
