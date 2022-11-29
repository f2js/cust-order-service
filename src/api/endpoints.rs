use crate::{models::orders::CreateOrder};
use super::utils::env::{get_db_ip};
use actix_web::{get, post, HttpResponse, Responder, web, HttpResponseBuilder};
use serde::Serialize;
use super::workers;
const DB_IP: &str = "165.22.194.124:9090";

#[post("/create")]
pub async fn create(param_obj: web::Json<CreateOrder>) -> impl Responder {
    match workers::create_order(param_obj, &DB_IP) {
        Ok(r) => 
            return generate_err_response(&mut HttpResponse::Ok(),format!("Successsfully added row: {:?}", r)),
        Err(e) => 
            return generate_err_response(&mut HttpResponse::InternalServerError(),e.to_string()),
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
            return generate_err_response(&mut HttpResponse::Ok(),tables),
        Err(e) => 
            return generate_err_response(&mut HttpResponse::InternalServerError(), e.to_string()),
    }
    
}

#[get("/order/{id}")]
pub async fn get_order(path: web::Path<String>) -> impl Responder {
    let id = path.into_inner();
    match workers::get_row(&id, &DB_IP) {
        Ok(r) => 
            return generate_err_response(&mut HttpResponse::Ok(),format!("Successfully got order: {:?}", r.o_id)),
        Err(e) =>
            return generate_err_response(&mut HttpResponse::InternalServerError(), e.to_string()),
    }
}

#[get("/cust/{id}")]
pub async fn get_orders_from_user(path: web::Path<String>) -> impl Responder {
    let id = path.into_inner();
    println!("{id}");
    match workers::get_orders_info_by_user(&id, &DB_IP) {
        Ok(r) => 
            return generate_err_response(&mut HttpResponse::Ok(), r),
        Err(e) =>
            return generate_err_response(&mut HttpResponse::InternalServerError(), e.to_string()),
    }
}

fn generate_err_response(response_builder: &mut HttpResponseBuilder, error: impl Serialize) -> HttpResponse {
    response_builder.content_type("APPLICATION_JSON").json(error)
}