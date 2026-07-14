FROM debian:trixie-slim

# Install necessary packages for iSCSI support
RUN apt-get update && apt-get install -y \
    util-linux \
    e2fsprogs \
    xfsprogs \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binaries (GoReleaser automatically copies them)
COPY rbs-csi-controller /bin/rbs-csi-controller
COPY rbs-csi-node /bin/rbs-csi-node

# Default entrypoint (can be overridden)
ENTRYPOINT ["/bin/rbs-csi-controller"]