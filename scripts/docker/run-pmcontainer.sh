#Provide start of the port range as command line arg
#I.e sudo ./run-cpcontainer.sh 5000
rm -rf *.raftdb configs ctl-interface logs
./raft-configs.sh $1
docker build -t pumice .
curdir=$(pwd)
docker compose up -d --build
