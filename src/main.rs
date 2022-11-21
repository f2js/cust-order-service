mod api;
mod models;
mod repository;

use std::io;
use actix_web::{App, HttpServer};

#[actix_web::main]
async fn main() -> io::Result<()> {

    HttpServer::new(|| {
        App::new()
            // register HTTP requests handlers
            .service(api::index)
            .service(api::get_tables)
            .service(api::create)
    })
    .bind("0.0.0.0:9090")?
    .run()
    .await
}