rm -rf /work/pumice
mkdir -p /work/pumice/db
mkdir -p /work/pumice/ctl
mkdir -p /work/pumice/logs
rm -rf configs
./raft-config.sh
cp -r configs /work/pumice/
cp -r lib /work/pumice/
cp -r libexec /work/pumice/
cp run.sh /work/pumice/
pdsh -w 192.168.96.8[4-8] /work/pumice/run.sh 
