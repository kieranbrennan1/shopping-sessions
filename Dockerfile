# Shopping Sessions: one Rust binary. Built natively on the scorer (linux/amd64).
FROM rust:1.90-slim-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Cascade Lake or newer: AVX2 is safe (x86-64-v3)
RUN if [ "$TARGETARCH" = "amd64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    cargo build --release && cp target/release/shopsess /shopsess

FROM debian:bookworm-slim
COPY --from=build /shopsess /app/shopsess
USER 1000:1000
CMD ["/app/shopsess"]
