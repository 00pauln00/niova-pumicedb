#Provide start of the port range as command line arg
#I.e sudo ./run-pumice.sh 5000
rm -rf *.raftdb configs ctl-interface logs
./raft-config.sh $1
mkdir -p ctl-interface
mkdir -p logs
docker build -t pumice .
curdir=$(pwd)
docker-compose up -d --build
