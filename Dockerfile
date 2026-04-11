FROM rust:1.85-slim AS builder

WORKDIR /app

# Cache dependencies: copy manifests first, build a dummy, then copy real src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release && rm -rf src target/release/deps/delta_explain*

COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim

LABEL org.opencontainers.image.source="https://github.com/cdelmonte-zg/delta-explain"
LABEL org.opencontainers.image.description="Make Delta pruning visible"
LABEL org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/delta-explain /usr/local/bin/delta-explain

ENTRYPOINT ["delta-explain"]
