FROM ubuntu:18.04

RUN apt-get update -y \
    && apt-get -y install libaio-dev openssl libk5crypto3 librocksdb5.8 uuid vim python3-pip \
    && pip3 install func_timeout sockets psutil dpath \
    && pip3 install ansible \
    && pip3 install jmespath
WORKDIR /opt
COPY holon /opt/bin/
RUN mkdir -p /opt/sbin/niova/

COPY pumicedb-server-test /opt/sbin/niova/
COPY pumicedb-client-test /opt/sbin/niova/
COPY raft-server /opt/sbin/niova/
COPY raft-client /opt/sbin/niova/

ENV ANSIBLE_LOOKUP_PLUGINS=/opt/bin/ansible
ENV PYTHONPATH=/opt/bin/ansible
ENV NIOVA_BIN_PATH=/opt/sbin/niova/

RUN ls -l /opt/bin/

#CMD ["/bin/bash", "-c", "cd /opt/bin/ansible && ansible-playbook -e 'srv_port=4000' -e 'npeers=5' -e 'dir_path=/tmp' -e 'client_port=14000' -e 'recipe=basic_ctl_int.yml' -e 'backend_type=pumicedb' holon.yml"]
