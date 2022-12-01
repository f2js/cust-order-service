FROM rust:1.65 AS builder
COPY . .
RUN cargo build --release

FROM debian:stable-slim
RUN apt-get update
RUN apt-get install -y openssl

COPY --from=builder ./target/release/order_service ./target/release/order_service
CMD ["./target/release/order_service"]
