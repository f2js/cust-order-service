// use crate::models::Order;
use crate::{repository::{hbase, hbase_connection::HbaseConnection}, models::orders::CreateOrder, models::orders::Order};
use actix_web::{get, post, HttpResponse, Responder, web};

const DB_IP: &str = "165.22.194.124:9090";

#[post("/create")]
pub async fn create(param_obj: web::Json<CreateOrder>) -> impl Responder {
    let con = match HbaseConnection::connect(&DB_IP) {
        Ok(r) => r, 
        Err(e) => return HttpResponse::InternalServerError().content_type("APPLICATION_JSON").json(e.to_string()),
    };
    let r = match hbase::add_order(Order::from(param_obj), con) {
        Ok(r) => r, 
        Err(e) => return HttpResponse::InternalServerError().content_type("APPLICATION_JSON").json(e.to_string()),
    };
    HttpResponse::Ok()
        .content_type("APPLICATION_JSON")
        .json(format!("Successsfully added row: {:?}", r))
}

#[get("/")]
pub async fn index() -> String {
    "Service is running".to_string()
}

#[get("/tables")]
pub async fn get_tables() -> impl Responder {
    let con = match HbaseConnection::connect(&DB_IP) {
        Ok(r) => r, 
        Err(e) => return HttpResponse::InternalServerError().content_type("APPLICATION_JSON").json(e.to_string()),
    };
    let tables = match hbase::get_tables(con) {
        Ok(r) => r, 
        Err(e) => return HttpResponse::InternalServerError().content_type("APPLICATION_JSON").json(e.to_string()),
    };
    HttpResponse::Ok()
        .content_type("APPLICATION_JSON")
        .json(tables)
}