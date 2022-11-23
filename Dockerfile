FROM rust:1.65 AS builder
COPY . .
RUN cargo build --release

FROM debian:buster-slim
EXPOSE 8080
COPY --from=builder ./target/release/order-service ./target/release/order-service
CMD ["./target/release/order-service"]