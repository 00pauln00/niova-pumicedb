FROM ubuntu:18.04

#Create dir for niovakv_server and pmdb_server
RUN apt-get update -y \
    && apt-get -y install librocksdb5.8 \
    && mkdir /usr/niovakv_server \
    && mkdir /usr/pmdb_server \
    && mkdir /opt/niova \
    && mkdir /opt/niova/lib

#copy libs,configs and executables
COPY libexec/niova/proxy /usr/niovakv_server
COPY libexec/niova/niovakv.config /usr/niovakv_server/config
COPY libexec/niova/pmdbServer /usr/pmdb_server
COPY starter.sh /usr/
ADD  raftconfig /usr/pmdb_server/raftconfig
ADD  lib /opt/niova/lib

#Set environmental variable
ENV NIOVA_LOCAL_CTL_SVC_DIR="/usr/pmdb_server/raftconfig/configs"
ENV LD_LIBRARY_PATH="/opt/niova/lib"

#Run pmdb server
WORKDIR /usr/
RUN chmod +x starter.sh
CMD ./starter.sh
