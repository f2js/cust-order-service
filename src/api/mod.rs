// use crate::models::Order;
use crate::{repository::hbase, models::orders::CreateOrder, models::orders::Order};
use actix_web::{get, post, HttpResponse, Responder, web};

#[post("/create")]
pub async fn create(param_obj: web::Json<CreateOrder>) -> impl Responder {
    let r = match hbase::add_order(Order::from(param_obj)) {
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
    let tables = match hbase::get_tables() {
        Ok(r) => r, 
        Err(e) => return HttpResponse::InternalServerError().content_type("APPLICATION_JSON").json(e.to_string()),
    };
    HttpResponse::Ok()
        .content_type("APPLICATION_JSON")
        .json(tables)
}