pub mod api;
pub mod models;
mod repository;

use actix_web::{App, HttpServer};

pub async fn run_api() -> std::io::Result<()>{
    HttpServer::new(|| {
        App::new()
            // register HTTP requests handlers
            .service(api::endpoints::index)
            .service(api::endpoints::get_tables)
            .service(api::endpoints::create)
            .service(api::endpoints::get_orders_from_user)
            .service(api::endpoints::get_order)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}