use actix_web::{web};

use crate::{models::{orders::{CreateOrder, Order}, tables::TableName}, repository::{hbase_connection::HbaseConnection, hbase}};

pub fn create_order(param_obj: web::Json<CreateOrder>, db_ip: &str) -> Result<String, thrift::Error> {
    let con = HbaseConnection::connect(db_ip)?;
    hbase::add_order(Order::from(param_obj), con)
}

pub fn get_tables(db_ip: &str) -> Result<Vec<TableName>, thrift::Error> {
    let con = HbaseConnection::connect(db_ip)?;
    hbase::get_tables(con)
}