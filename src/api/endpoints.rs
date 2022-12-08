use crate::{models::orders::CreateOrder, api::utils::env::{get_db_ip, get_kafka_ip, DB_IP_ENV_ERR_MSG, KAFKA_IP_ENV_ERR_MSG}};
use actix_web::{get, post, HttpResponse, Responder, web, HttpResponseBuilder};
use serde::Serialize;
use super::workers;
// const DB_IP: &str = "165.22.194.124:9090";

#[get("/")]
pub async fn index() -> String {
    "Service is running".to_string()
}

#[post("/create")]
pub async fn create(param_obj: web::Json<CreateOrder>) -> impl Responder {
    let db_ip = match get_db_ip() {
        Some(v) => v,
        None => return generate_response(&mut HttpResponse::InternalServerError(), DB_IP_ENV_ERR_MSG),
    };
    let kafka_ip = match get_kafka_ip() {
        Some(v) => v,
        None => return generate_response(&mut HttpResponse::InternalServerError(), KAFKA_IP_ENV_ERR_MSG),
    };
    let order = match workers::create_order(param_obj, &db_ip, &kafka_ip) {
        Ok(r) => 
            r,
        Err(e) => 
            return generate_response(&mut HttpResponse::InternalServerError(),e.to_string()),
    };
    let jsonstring = match order.to_json_string() {
        Ok(r) => r,
        Err(e) => return generate_response(&mut HttpResponse::InternalServerError(),e.to_string()),
    };
    generate_response(&mut HttpResponse::Ok(),jsonstring)
}


#[get("/tables")]
pub async fn get_tables() -> impl Responder {
    let db_ip = match get_db_ip() {
        Some(v) => v,
        None => return generate_response(&mut HttpResponse::InternalServerError(), DB_IP_ENV_ERR_MSG),
    };
    match workers::get_tables(&db_ip) {
        Ok(tables) => 
            return generate_response(&mut HttpResponse::Ok(),tables),
        Err(e) => 
            return generate_response(&mut HttpResponse::InternalServerError(), e.to_string()),
    }
    
}

#[get("/order/{id}")]
pub async fn get_order(path: web::Path<String>) -> impl Responder {
    let db_ip = match get_db_ip() {
        Some(v) => v,
        None => return generate_response(&mut HttpResponse::InternalServerError(), DB_IP_ENV_ERR_MSG),
    };
    let id = path.into_inner();
    match workers::get_row(&id, &db_ip) {
        Ok(r) => 
            return generate_response(&mut HttpResponse::Ok(),format!("Successfully got order: {:?}", r.o_id)),
        Err(e) =>
            return generate_response(&mut HttpResponse::InternalServerError(), e.to_string()),
    }
}

#[get("/cust/{id}")]
pub async fn get_orders_from_user(path: web::Path<String>) -> impl Responder {
    let db_ip = match get_db_ip() {
        Some(v) => v,
        None => return generate_response(&mut HttpResponse::InternalServerError(), DB_IP_ENV_ERR_MSG),
    };
    let id = path.into_inner();
    match workers::get_orders_info_by_user(&id, &db_ip) {
        Ok(r) => 
            return generate_response(&mut HttpResponse::Ok(), r),
        Err(e) =>
            return generate_response(&mut HttpResponse::InternalServerError(), e.to_string()),
    }
}

fn generate_response(response_builder: &mut HttpResponseBuilder, val: impl Serialize) -> HttpResponse {
    response_builder.content_type("APPLICATION_JSON").json(val)
}