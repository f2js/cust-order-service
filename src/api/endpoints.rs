use super::workers;
use crate::{
    api::utils::env::{get_db_ip, get_kafka_ip, DB_IP_ENV_ERR_MSG, KAFKA_IP_ENV_ERR_MSG},
    models::orders::CreateOrder, models::errors::OrderServiceError,
};
use actix_web::{get, post, web, HttpResponse, HttpResponseBuilder, Responder};
use serde::Serialize;
// const DB_IP: &str = "165.22.194.124:9090";

#[get("/")]
pub async fn index() -> String {
    "Service is running".to_string()
}

#[post("/create")]
pub async fn create(param_obj: web::Json<CreateOrder>) -> impl Responder {
    let db_ip = match get_db_ip() {
        Some(v) => v,
        None => {
            return generate_response(&mut HttpResponse::InternalServerError(), DB_IP_ENV_ERR_MSG)
        }
    };
    let kafka_ip = match get_kafka_ip() {
        Some(v) => v,
        None => {
            return generate_response(
                &mut HttpResponse::InternalServerError(),
                KAFKA_IP_ENV_ERR_MSG,
            )
        }
    };
    let order = match workers::create_order(param_obj, &db_ip, &kafka_ip) {
        Ok(r) => r,
        Err(e) => {
            return generate_response(&mut HttpResponse::InternalServerError(), e.to_string())
        }
    };
    generate_response(&mut HttpResponse::Ok(), order)
}

#[get("/tables")]
pub async fn get_tables() -> impl Responder {
    let db_ip = match get_db_ip() {
        Some(v) => v,
        None => {
            return generate_response(&mut HttpResponse::InternalServerError(), DB_IP_ENV_ERR_MSG)
        }
    };
    match workers::get_tables(&db_ip) {
        Ok(tables) => return generate_response(&mut HttpResponse::Ok(), tables),
        Err(e) => {
            return generate_response(&mut HttpResponse::InternalServerError(), e.to_string())
        }
    }
}

#[get("/order/{id}")]
pub async fn get_order(path: web::Path<String>) -> impl Responder {
    let db_ip = match get_db_ip() {
        Some(v) => v,
        None => {
            return generate_response(&mut HttpResponse::InternalServerError(), DB_IP_ENV_ERR_MSG)
        }
    };
    let id = path.into_inner();
    let order = match workers::get_row(&id, &db_ip) {
        Ok(r) => r,
        Err(e) => {
            match e {
                OrderServiceError::RowNotFound(r) => return generate_response(&mut HttpResponse::NotFound(), format!("Order by id {} was not found.", r)),
                _ => return generate_response(&mut HttpResponse::InternalServerError(), e.to_string())
            }
        }
    };
    generate_response(&mut HttpResponse::Ok(), order)
}

#[get("/cust/{id}")]
pub async fn get_orders_from_user(path: web::Path<String>) -> impl Responder {
    let db_ip = match get_db_ip() {
        Some(v) => v,
        None => {
            return generate_response(&mut HttpResponse::InternalServerError(), DB_IP_ENV_ERR_MSG)
        }
    };
    let id = path.into_inner();
    let r = match workers::get_orders_info_by_user(&id, &db_ip) {
        Ok(r) => r,
        Err(e) => {
            return generate_response(&mut HttpResponse::InternalServerError(), e.to_string())
        }
    };
    if r.is_empty() {
        return generate_response(&mut HttpResponse::NotFound(), format!("No orders was found for customer with id {id}."));
    }
    generate_response(&mut HttpResponse::Ok(), r)
}

fn generate_response(
    response_builder: &mut HttpResponseBuilder,
    val: impl Serialize,
) -> HttpResponse {
    response_builder.content_type("APPLICATION_JSON").json(val)
}
