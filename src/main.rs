use order_service::run_api;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    run_api().await
}