FROM ubuntu:24.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    clang \
    cmake \
    nasm \
    libc++-dev \
    libc++abi-dev \
    libjsoncpp-dev \
    uuid-dev \
    zlib1g-dev \
    libssl-dev \
    libhiredis-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Clone and build
WORKDIR /build
ARG REPO_URL=https://github.com/qubic/core-lite.git
ARG BRANCH=main

RUN git clone --depth 1 --branch ${BRANCH} ${REPO_URL} qubic-core-lite

WORKDIR /build/qubic-core-lite/build

RUN cmake .. \
    -D CMAKE_C_COMPILER=clang \
    -D CMAKE_CXX_COMPILER=clang++ \
    -D BUILD_TESTS:BOOL=OFF \
    -D BUILD_BINARY:BOOL=ON \
    -D CMAKE_BUILD_TYPE=Release \
    -D ENABLE_AVX512=OFF \
    && cmake --build . --target Qubic -- -j$(nproc)

# Extract epoch from source
RUN grep -oP '#define EPOCH \K[0-9]+' /build/qubic-core-lite/src/public_settings.h > /build/epoch.txt

# Runtime image
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    libc++1 \
    libc++abi1 \
    libjsoncpp25 \
    libssl3 \
    libhiredis1.1.0 \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /qubic

# Copy binary and epoch info from builder
COPY --from=builder /build/qubic-core-lite/build/src/Qubic /qubic/Qubic
COPY --from=builder /build/epoch.txt /qubic/epoch.txt

# Copy entrypoint script
COPY entrypoint.sh /qubic/entrypoint.sh
RUN chmod +x /qubic/entrypoint.sh /qubic/Qubic

# Default Qubic mainnet port
EXPOSE 21841

# Environment variables
ENV EPOCH=""
ENV PEERS=""
ENV SECURITY_TICK="32"
ENV FORCE_DOWNLOAD=""

ENTRYPOINT ["/qubic/entrypoint.sh"]
