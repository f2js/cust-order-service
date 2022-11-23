use crate::{models::orders::CreateOrder};
use actix_web::{get, post, HttpResponse, Responder, web};
use super::workers;
const DB_IP: &str = "165.22.194.124:9090";

#[post("/create")]
pub async fn create(param_obj: web::Json<CreateOrder>) -> impl Responder {
    match workers::create_order(param_obj, &DB_IP) {
        Ok(r) => 
            return HttpResponse::Ok()
                    .content_type("APPLICATION_JSON")
                    .json(format!("Successsfully added row: {:?}", r)),
        Err(e) => 
            return HttpResponse::InternalServerError()
                    .content_type("APPLICATION_JSON")
                    .json(e.to_string()),
    };
}

#[get("/")]
pub async fn index() -> String {
    "Service is running".to_string()
}

#[get("/tables")]
pub async fn get_tables() -> impl Responder {
    match workers::get_tables(&DB_IP) {
        Ok(tables) => 
            return HttpResponse::Ok()
                .content_type("APPLICATION_JSON")
                .json(tables),
        Err(e) => 
            return HttpResponse::InternalServerError()
                .content_type("APPLICATION_JSON")
                .json(e.to_string()),
    }
    
}