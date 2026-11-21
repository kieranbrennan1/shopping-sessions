# Shopping Sessions: one Rust binary. Built natively on the scorer (linux/amd64).
FROM rust:1.90-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release && cp target/release/shopsess /shopsess

FROM debian:bookworm-slim
COPY --from=build /shopsess /app/shopsess
USER 1000:1000
CMD ["/app/shopsess"]
