extern crate order_service;

use actix_web::web::Json;
use cucumber::{given, then, when, World, Parameter};
use order_service::{repository::hbase_connection::HbaseConnection, api::{utils::env::get_env_var, workers}, models::orders::{Orderline, CreateOrder}};
use order_service::models::errors::OrderServiceError;
use order_service::models::orders::Order;

#[derive(World, Debug, Default, Clone)]
pub struct State {
    expected: Option<CreateOrder>,
    input: Option<(String, String)>,
    output: Option<Order>,
}

fn main() {
    futures::executor::block_on(State::run("features/"));
}

#[given(expr = "we have an order database")]
fn given_db_ip(s: &mut State) {
    let hbip = get_env_var("HBASE_TEST_IP").unwrap();
    let kafip = get_env_var("KAFKA_TEST_IP").unwrap();
    s.input = Some((hbip, kafip));
}

#[when(expr = "we create a new order")]
fn when_(s: &mut State) {
    let (hbip, kafip) = s.input.clone().unwrap();
    let ol1 = Orderline {
        item_num: 10,
        price: 5,
    };
    let order_to_create = CreateOrder {
        c_id: "CustomerId".into(),
        r_id: "RestaurantId".into(),
        cust_addr: "CustomerAddress".into(),
        rest_addr: "RestaurantAddress".into(),
        postal_code: 2860,
        orderlines: vec![ol1.clone()],
    };
    s.expected = Some(order_to_create.clone());

    let res = workers::create_order(
        Json(order_to_create.clone()), 
        &hbip, 
        &kafip
    ).unwrap();
    s.output = Some(res);
}

#[then(expr = "the order is created in the database")]
fn then_(s: &mut State) {
    let (hbip, _) = s.input.clone().unwrap();
    let expected = s.expected.clone().unwrap();
    let order = s.output.clone().unwrap();
    let res = workers::get_row(&order.o_id, &hbip).unwrap();
    assert_eq!(res.c_id, expected.c_id);
    assert_eq!(res.r_id, expected.r_id);
    assert_eq!(res.cust_addr, expected.cust_addr);
    assert_eq!(res.rest_addr, expected.rest_addr);
    assert_eq!(res.orderlines.len(), expected.orderlines.len());
    assert_eq!(res.orderlines[0].item_num, expected.orderlines[0].item_num);
    assert_eq!(res.orderlines[0].price, expected.orderlines[0].price);
}