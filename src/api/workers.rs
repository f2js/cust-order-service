use actix_web::{web};

use crate::{models::{orders::{CreateOrder, Order, OrderInfo}, tables::TableName, errors::OrderServiceError}, repository::{hbase_connection::HbaseConnection, hbase}, producers::{producers, producer_connection::KafkaProdConnection}};

pub fn create_order(param_obj: web::Json<CreateOrder>, db_ip: &str, kafka_ip: &str) -> Result<String, OrderServiceError> {
    let hbase_con = HbaseConnection::connect(db_ip)?;
    let order = Order::from(param_obj);
    let o_id = hbase::add_order(order.clone(), hbase_con)?;
    if !kafka_ip.eq("-1") { // For testing without kafka
        let mut kafka_con = KafkaProdConnection::connect(kafka_ip.into())?;
        producers::publish_order_created(order, &mut kafka_con)?;
    }
    Ok(o_id)
}

pub fn get_tables(db_ip: &str) -> Result<Vec<TableName>, OrderServiceError> {
    let con = HbaseConnection::connect(db_ip)?;
    hbase::get_tables(con)
}

pub fn create_table(db_ip: &str) -> Result<(), OrderServiceError> {
    let con = HbaseConnection::connect(db_ip)?;
    hbase::create_order_table(con)
}

pub fn get_row(row_id: &str, db_ip: &str) -> Result<Order, OrderServiceError> {
    let con = HbaseConnection::connect(db_ip)?;
    hbase::get_order_row(row_id, con)
}

pub fn get_orders_info_by_user(user_id: &str, db_ip: &str) -> Result<Vec<OrderInfo>, OrderServiceError> {
    let con = HbaseConnection::connect(db_ip)?;
    hbase::get_orders_info_by_user(user_id.to_string(), con)
}