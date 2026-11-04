# Redis whose data directory is a LazyFS mount. LazyFS (a FUSE file system)
# keeps written data in its own cache until the application fsyncs it, and the
# command "lazyfs::clear-cache" drops everything that was never fsynced: what a
# power loss or kernel crash does to the page cache. crash.sh uses it.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      g++ cmake make git ca-certificates libfuse3-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*
# LazyFS 0.3.1
ARG LAZYFS_COMMIT=045a0b3a1126725e693934e29d3ba15e08cc39ec
RUN git clone https://github.com/dsrhaslab/lazyfs /src \
 && git -C /src checkout --quiet "$LAZYFS_COMMIT" \
 && cd /src/libs/libpcache && ./build.sh \
 && cd /src/lazyfs && ./build.sh

FROM redis:7.4.12-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends fuse3 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/lazyfs/build/lazyfs /usr/local/bin/lazyfs
COPY --from=build /src/libs/libpcache/build/libpcache.so /usr/local/lib/
RUN ldconfig
COPY entrypoint.sh crash.sh /usr/local/bin/
RUN chmod 0755 /usr/local/bin/entrypoint.sh /usr/local/bin/crash.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
