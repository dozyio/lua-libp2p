FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  git \
  cmake \
  build-essential \
  pkg-config \
  libssl-dev \
  lua5.4 \
  liblua5.4-dev \
  luarocks \
  libsodium-dev \
  redis-tools \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN luarocks --lua-version=5.4 make lua-libp2p-0.1.0-1.rockspec

ENTRYPOINT ["lua5.4", "interop/transport/main.lua"]
