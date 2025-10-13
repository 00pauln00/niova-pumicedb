FROM ubuntu:noble

# Install dependencies
RUN apt-get update -y \
    && apt-get -y install uuid-runtime librocksdb8.9 \
    && apt-get clean

WORKDIR /pumice

# Environment variables
ENV NIOVA_LOCAL_CTL_SVC_DIR="/pumice/configs"
ENV LD_LIBRARY_PATH="/lib:/pumice/lib"
ENV NIOVA_INOTIFY_BASE_PATH="/pumice/ctl-interface"

# Copy pumice scripts
COPY pumice.sh ./ 
COPY libexec ./libexec
COPY configs ./configs

# Make scripts executable
RUN chmod +x pumice.sh

ENTRYPOINT ["/pumice/pumice.sh"]
