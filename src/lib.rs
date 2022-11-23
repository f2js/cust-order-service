pub mod api;
mod models;
mod repository;

use actix_web::{App, HttpServer};

pub async fn run_api() -> std::io::Result<()>{
    HttpServer::new(|| {
        App::new()
            // register HTTP requests handlers
            .service(api::endpoints::index)
            .service(api::endpoints::get_tables)
            .service(api::endpoints::create)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}