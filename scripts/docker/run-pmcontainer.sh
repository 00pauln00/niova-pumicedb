#Provide start of the port range as command line arg
#I.e sudo ./run-cpcontainer.sh 5000
rm -rf *.raftdb configs ctl-interface logs
./raft-config.sh $1
mkdir -p ctl-interface
docker build -t pumice .
curdir=$(pwd)
docker-compose up -d --build
