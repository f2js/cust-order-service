FROM rust:1.65 AS builder
COPY . .
RUN cargo build --release

FROM debian:buster-slim
EXPOSE 9090
COPY --from=builder ./target/release/order-service ./target/release/order-service
CMD ["./target/release/order-service"]