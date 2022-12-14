#[cfg(test)]
mod integration_tests {
    extern crate order_service;
    use std::collections::HashMap;

    use actix_web::web::Json;
    use testcontainers::{core::WaitFor, images::generic::GenericImage, *};

    use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

    use order_service::{
        api::{workers::{self, create_table}, utils::env::get_env_var},
        models::orders::{CreateOrder, Orderline, Order},
        repository::{hbase, hbase_connection::HbaseConnection},
    };

    macro_rules! start_hbase_container_and_create_table {
        ($docker: expr) => {{
            let wait_for = WaitFor::message_on_stdout("server.Server: Started");
            let image = GenericImage::new("harisekhon/hbase", "2.0")
                .with_exposed_port(9090)
                .with_wait_for(wait_for.clone());
            let hbase = $docker.run(image);

            let mut ip = String::from("127.0.0.1");
            let port = hbase.get_host_port_ipv4(9090).to_string();
            ip.push(':');
            ip.push_str(&port);
            println!("Started container at IP: {:?}", ip);
            std::thread::sleep(std::time::Duration::from_secs(5)); // no clue why this makes it work
            let res = create_table(&ip);
            match res {
                Ok(_) => Ok((hbase, ip)),
                Err(e) => {
                    println!("Error!: {:?}", e.to_string());
                    Err(e)
                }
            }
        }};
    }

    macro_rules! start_kafka_container_and_create_topic {
        ($docker: expr) => {{
            let img = images::kafka::Kafka::default();
            let runnableimg =
                RunnableImage::from(img).with_env_var(("KAFKA_CREATE_TOPICS", "OrderCreated"));
            let kafka_node = $docker.run(runnableimg);

            let ip = format!(
                "127.0.0.1:{}",
                kafka_node.get_host_port_ipv4(images::kafka::KAFKA_PORT)
            );
            std::thread::sleep(std::time::Duration::from_secs(5));
            (kafka_node, ip)
        }};
    }

    #[test]
    fn integration_test_add_order_on_db_content() {
        let ol1 = Orderline {
            item_num: 10,
            price: 5,
        };
        let ol2 = Orderline {
            item_num: 16,
            price: 32,
        };
        let ol3 = Orderline {
            item_num: 20,
            price: 64,
        };
        let order_to_create = CreateOrder {
            c_id: "CustomerId".into(),
            r_id: "RestaurantId".into(),
            cust_addr: "CustomerAddress".into(),
            rest_addr: "RestaurantAddress".into(),
            postal_code: 2860,
            orderlines: vec![ol1.clone(), ol2.clone(), ol3.clone()],
        };
        let hbip = get_env_var("HBASE_TEST_IP").unwrap();
        let hbase_con = HbaseConnection::connect(&hbip).unwrap();
        let o_id = hbase::add_order(
            &Order::from(Json(order_to_create.clone())), 
            hbase_con
        ).unwrap();
        
        let res = workers::get_row(&o_id, &hbip).unwrap();
        assert_eq!(res.c_id, order_to_create.c_id);
        assert_eq!(res.r_id, order_to_create.r_id);
        assert_eq!(res.cust_addr, order_to_create.cust_addr);
        assert_eq!(res.rest_addr, order_to_create.rest_addr);
        assert_eq!(res.orderlines.len(), order_to_create.orderlines.len());
        assert_eq!(res.orderlines[0].item_num, ol1.item_num);
        assert_eq!(res.orderlines[1].item_num, ol2.item_num);
        assert_eq!(res.orderlines[2].item_num, ol3.item_num);
        assert_eq!(res.orderlines[0].price, ol1.price);
        assert_eq!(res.orderlines[1].price, ol2.price);
        assert_eq!(res.orderlines[2].price, ol3.price);
    }

    #[test]
    fn integration_test_add_order_on_returned() {
        //Arrange
        let ol1 = Orderline {
            item_num: 10,
            price: 5,
        };
        let ol2 = Orderline {
            item_num: 16,
            price: 32,
        };
        let ol3 = Orderline {
            item_num: 20,
            price: 64,
        };
        let order_to_create = CreateOrder {
            c_id: "CustomerId".into(),
            r_id: "RestaurantId".into(),
            cust_addr: "CustomerAddress".into(),
            rest_addr: "RestaurantAddress".into(),
            postal_code: 2860,
            orderlines: vec![ol1.clone(), ol2.clone(), ol3.clone()],
        };
        let hbip = get_env_var("HBASE_TEST_IP").unwrap();
        let kafip = get_env_var("KAFKA_TEST_IP").unwrap();
        //Act
        let res = workers::create_order(
            Json(order_to_create.clone()), 
            &hbip, 
            &kafip
        );

        //Assert
        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(res.c_id, order_to_create.c_id);
        assert_eq!(res.r_id, order_to_create.r_id);
        assert_eq!(res.cust_addr, order_to_create.cust_addr);
        assert_eq!(res.rest_addr, order_to_create.rest_addr);
        assert_eq!(res.orderlines.len(), order_to_create.orderlines.len());
        assert_eq!(res.orderlines[0].item_num, ol1.item_num);
        assert_eq!(res.orderlines[1].item_num, ol2.item_num);
        assert_eq!(res.orderlines[2].item_num, ol3.item_num);
        assert_eq!(res.orderlines[0].price, ol1.price);
        assert_eq!(res.orderlines[1].price, ol2.price);
        assert_eq!(res.orderlines[2].price, ol3.price);
    }

    #[test]
    #[ignore = "This test is expensive, and does not work when test is run in docker container. Use 'cargo test -- --ignored' to run ignored tests."]
    fn component_test_get_order_by_user() {
        let docker = clients::Cli::docker();
        let (_hbase, ip) = start_hbase_container_and_create_table!(docker).unwrap();
        // let (kafka, kaf_ip) = start_kafka_container_and_create_topic!(docker);
        let cust_id = "CustomerId";
        let order_to_create1 = CreateOrder {
            c_id: cust_id.clone().into(),
            r_id: "RestaurantId".into(),
            cust_addr: "CustomerAddress".into(),
            rest_addr: "RestaurantAddress".into(),
            postal_code: 2860,
            orderlines: vec![],
        };
        let order_to_create2 = CreateOrder {
            c_id: cust_id.clone().into(),
            r_id: "otherrest".into(),
            cust_addr: "CustomerAddress".into(),
            rest_addr: "otheraddresss".into(),
            postal_code: 2860,
            orderlines: vec![Orderline {
                item_num: 1,
                price: 5,
            }],
        };
        let order_to_create3 = CreateOrder {
            c_id: cust_id.clone().into(),
            r_id: "otherrest".into(),
            cust_addr: "CustomerAddress".into(),
            rest_addr: "otheraddresss".into(),
            postal_code: 2860,
            orderlines: vec![Orderline {
                item_num: 1,
                price: 5,
            }],
        };
        let x = workers::create_order(Json(order_to_create1.clone()), &ip, "localhost:9092").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(5));
        let y = workers::create_order(Json(order_to_create2.clone()), &ip, "localhost:9092").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(5));
        let z = workers::create_order(Json(order_to_create3.clone()), &ip, "localhost:9092").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(5));
        let res = workers::get_orders_info_by_user(cust_id, &ip).unwrap();
        println!("{}", res.len());
        assert!(res.len() == 3);
    }

    #[test]
    #[ignore = "This test is expensive, and does not work when test is run in docker container. Use 'cargo test -- --ignored' to run ignored tests."]
    fn component_create_order_empty() {
        let docker = clients::Cli::docker();
        let (hbase, ip) = start_hbase_container_and_create_table!(docker).unwrap();
        // let (kafka,  kaf_ip) = start_kafka_container_and_create_topic!(docker);

        let order_to_create = CreateOrder {
            c_id: "CustomerId".into(),
            r_id: "RestaurantId".into(),
            cust_addr: "CustomerAddress".into(),
            rest_addr: "RestaurantAddress".into(),
            postal_code: 2860,
            orderlines: vec![],
        };
        let o = workers::create_order(Json(order_to_create.clone()), &ip, "localhost:9092").unwrap();
        let res = workers::get_row(&o.o_id, &ip).unwrap();
        assert_eq!(res.c_id, order_to_create.c_id);
        assert_eq!(res.r_id, order_to_create.r_id);
        assert_eq!(res.cust_addr, order_to_create.cust_addr);
        assert_eq!(res.rest_addr, order_to_create.rest_addr);
        assert_eq!(res.orderlines.len(), order_to_create.orderlines.len());
    }

    #[test]
    #[ignore = "This test is expensive, and does not work when test is run in docker container. Use 'cargo test -- --ignored' to run ignored tests."]
    fn component_create_order() {
        let docker = clients::Cli::docker();
        let (hbase, ip) = start_hbase_container_and_create_table!(docker).unwrap();
        // let (kafka,  kaf_ip) = start_kafka_container_and_create_topic!(docker);

        let ol1 = Orderline {
            item_num: 10,
            price: 5,
        };
        let ol2 = Orderline {
            item_num: 16,
            price: 32,
        };
        let ol3 = Orderline {
            item_num: 20,
            price: 64,
        };
        let order_to_create = CreateOrder {
            c_id: "CustomerId".into(),
            r_id: "RestaurantId".into(),
            cust_addr: "CustomerAddress".into(),
            rest_addr: "RestaurantAddress".into(),
            postal_code: 2860,
            orderlines: vec![ol1.clone(), ol2.clone(), ol3.clone()],
        };
        let o = workers::create_order(Json(order_to_create.clone()), &ip, "localhost:9092").unwrap();
        let res = workers::get_row(&o.o_id, &ip).unwrap();
        assert_eq!(res.c_id, order_to_create.c_id);
        assert_eq!(res.r_id, order_to_create.r_id);
        assert_eq!(res.cust_addr, order_to_create.cust_addr);
        assert_eq!(res.rest_addr, order_to_create.rest_addr);
        assert_eq!(res.orderlines.len(), order_to_create.orderlines.len());
        for ol in res.orderlines.iter() {
            assert!(
                ol.item_num == ol1.item_num
                    || ol.item_num == ol2.item_num
                    || ol.item_num == ol3.item_num
            );
            assert!(ol.price == ol1.price || ol.price == ol2.price || ol.price == ol3.price);
        }
    }

    #[test]
    #[ignore = "This test is expensive, and does not work when test is run in docker container. Use 'cargo test -- --ignored' to run ignored tests."]
    fn component_test_get_tables() {
        let docker = clients::Cli::docker();
        let (hbase, ip) = start_hbase_container_and_create_table!(docker).unwrap();
        let res = match workers::get_tables(&ip) {
            Ok(r) => r,
            Err(e) => {
                println!("Error!: {:?}", e.to_string());
                panic!("Booooo")
            }
        };
        for table in res {
            assert_eq!(table.table_name, "orders");
        }
    }
}
