FROM ubuntu:21.10

#Create dir for niovakv_server and pmdb_server
RUN apt-get update -y \
    && apt-get -y install librocksdb6.10 \
    && mkdir /usr/pmdb_server \
    && mkdir /opt/niova \
    && mkdir /opt/niova/lib

#copy libs,configs and executables
COPY pmdbServer /usr/pmdb_server
COPY gossipNodes /usr/pmdb_server
COPY PMDBServerStarter.sh /usr/starter.sh
ADD  raftconfig /usr/pmdb_server/raftconfig
ADD  lib /opt/niova/lib

#Set environmental variable
ENV NIOVA_LOCAL_CTL_SVC_DIR="/usr/pmdb_server/raftconfig/configs"
ENV LD_LIBRARY_PATH="/opt/niova/lib"

#Run pmdb server
WORKDIR /usr/
RUN chmod +x starter.sh
CMD ./starter.sh
