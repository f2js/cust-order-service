use hbase_thrift::{hbase::{BatchMutation}, MutationBuilder, BatchMutationBuilder};

use rand::prelude::*;
use rand_seeder::{Seeder};
use rand_pcg::Pcg64;

use crate::models::{orders::Order};

pub(crate) fn create_mutation_from_order(order: &Order) -> (BatchMutation, String) {
    let id_mut = create_cell_mutation("info", "o_id", order.o_id.to_string());
    let otime_mut = create_cell_mutation("info", "o_time", order.ordertime.to_string());
    let state_mut = create_cell_mutation("info", "state", order.state.to_string());
    let cid_mut = create_cell_mutation("ids", "c_id", order.c_id.clone());
    let rid_mut = create_cell_mutation("ids", "r_id", order.r_id.clone());
    let caddr_mut = create_cell_mutation("addr", "c_addr", order.cust_addr.clone());
    let raddr_mut = create_cell_mutation("addr", "r_addr", order.rest_addr.clone());
    
    let mut mutations = vec![id_mut, otime_mut, state_mut, cid_mut, rid_mut, caddr_mut, raddr_mut];
    for (i, orderline) in order.orderlines.iter().enumerate() {
        let orderline = create_cell_mutation("ol", i.to_string(), format!("{:?}:{:?}", orderline.item_num, orderline.price));
        mutations.push(orderline);
    }
    let rowkey = generate_row_key(&order);
    (<BatchMutationBuilder>::default().row(rowkey.clone()).mutations(mutations).build(), rowkey)
}


fn create_cell_mutation(column_family: impl Into<String>, column: impl Into<String>,  value: impl Into<Vec<u8>>) -> MutationBuilder {
    let mut mutation = MutationBuilder::default();
    mutation.column(column_family, column);
    mutation.value(value);
    mutation
}

fn generate_row_key(order: &Order) -> String {
    let mut res = String::from(generate_salt(&order.r_id));
    res.push_str(&order.o_id.to_string());
    res
}

fn generate_salt(seed: &str) -> String {
    let mut rng: Pcg64 = Seeder::from(seed).make_rng();
    rng.gen::<u8>().to_string()
}

#[cfg(test)]
mod tests {

    use super::{generate_salt, generate_row_key, create_cell_mutation, create_mutation_from_order};
    use crate::models::orders::{Order, Orderline};
    #[test]
    fn test_create_mutation_from_order_full_columns() {
        let ol1 = Orderline{item_num: 10, price: 5};
        let ol2 = Orderline{item_num: 16, price: 32};
        let ol3 = Orderline{item_num: 20, price: 64};
        let order = Order::new(vec![ol1.clone(), ol2.clone(), ol3.clone()], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, _) = create_mutation_from_order(&order);
        let mut mutations = bmut.mutations.unwrap();
        let ol3_mut = mutations.pop().unwrap();
        let ol2_mut = mutations.pop().unwrap();
        let ol1_mut   = mutations.pop().unwrap();
        let raddr_mut = mutations.pop().unwrap();
        let caddr_mut = mutations.pop().unwrap();
        let rid_mut   = mutations.pop().unwrap();
        let cid_mut   = mutations.pop().unwrap();
        let state_mut = mutations.pop().unwrap();
        let otime_mut = mutations.pop().unwrap();
        let o_id_mut  = mutations.pop().unwrap();

        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ol", "2"));
        assert_eq!(ol3_mut.column.unwrap(), exp_cols, "Column family or Column for orderline3 did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ol", "1"));
        assert_eq!(ol2_mut.column.unwrap(), exp_cols, "Column family or Column for orderline2 did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ol", "0"));
        assert_eq!(ol1_mut.column.unwrap(), exp_cols, "Column family or Column for orderline1 did not match the expected names.");
        
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("addr", "r_addr"));
        assert_eq!(raddr_mut.column.unwrap(), exp_cols, "Column family or Column for restaurant address did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("addr", "c_addr"));
        assert_eq!(caddr_mut.column.unwrap(), exp_cols, "Column family or Column for customer address did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ids", "r_id"));
        assert_eq!(rid_mut.column.unwrap(), exp_cols, "Column family or Column for restaurant id did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ids", "c_id"));
        assert_eq!(cid_mut.column.unwrap(), exp_cols, "Column family or Column for customer id did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("info", "state"));
        assert_eq!(state_mut.column.unwrap(), exp_cols, "Column family or Column for state did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("info", "o_time"));
        assert_eq!(otime_mut.column.unwrap(), exp_cols, "Column family or Column for ordertime did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("info", "o_id"));
        assert_eq!(o_id_mut.column.unwrap(), exp_cols, "Column family or Column for order id did not match the expected names.");
    }

    #[test]
    fn test_create_mutation_from_order_full_values() {
        let ol1 = Orderline{item_num: 10, price: 5};
        let ol2 = Orderline{item_num: 16, price: 32};
        let ol3 = Orderline{item_num: 20, price: 64};
        let order = Order::new(vec![ol1.clone(), ol2.clone(), ol3.clone()], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, _) = create_mutation_from_order(&order);
        let mut mutations = bmut.mutations.unwrap();
        let ol3_mut = mutations.pop().unwrap();
        let ol2_mut = mutations.pop().unwrap();
        let ol1_mut   = mutations.pop().unwrap();
        let raddr_mut = mutations.pop().unwrap();
        let caddr_mut = mutations.pop().unwrap();
        let rid_mut   = mutations.pop().unwrap();
        let cid_mut   = mutations.pop().unwrap();
        let state_mut = mutations.pop().unwrap();
        let otime_mut = mutations.pop().unwrap();
        let o_id_mut  = mutations.pop().unwrap();

        let exp_val: Vec<u8> = tuple_to_u8_vec((&ol3.item_num.to_string(), &ol3.price.to_string()));
        assert_eq!(ol3_mut.value.unwrap(), exp_val, "Orderline3 value did not match expected value");
        let exp_val: Vec<u8> = tuple_to_u8_vec((&ol2.item_num.to_string(), &ol2.price.to_string()));
        assert_eq!(ol2_mut.value.unwrap(), exp_val, "Orderline2 value did not match expected value");
        let exp_val: Vec<u8> = tuple_to_u8_vec((&ol1.item_num.to_string(), &ol1.price.to_string()));
        assert_eq!(ol1_mut.value.unwrap(), exp_val, "Orderline1 value did not match expected value");

        let exp_raddr: Vec<u8> = order.rest_addr.into();
        assert_eq!(raddr_mut.value.unwrap(), exp_raddr, "Restaurant Address did not match the expected address.");
        let exp_caddr: Vec<u8> = order.cust_addr.into();
        assert_eq!(caddr_mut.value.unwrap(), exp_caddr, "Customer Address did not match the expected address.");
        let exp_rid: Vec<u8> = order.r_id.into();
        assert_eq!(rid_mut.value.unwrap(), exp_rid, "Restaurant ID did not match the expected ID.");
        let exp_cid: Vec<u8> = order.c_id.into();
        assert_eq!(cid_mut.value.unwrap(), exp_cid, "Customer ID did not match the expected ID.");
        let exp_state: Vec<u8> = order.state.to_string().into();
        assert_eq!(state_mut.value.unwrap(), exp_state, "State did not match the expected State.");
        let exp_otime: Vec<u8> = order.ordertime.to_string().into();
        assert_eq!(otime_mut.value.unwrap(), exp_otime, "Ordertime did not match the expected Ordertime.");
        let exp_o_id: Vec<u8> = order.o_id.to_string().into();
        assert_eq!(o_id_mut.value.unwrap(), exp_o_id, "OrderId did not match the expected OrderId.");
    }
    #[test]
    fn test_create_mutation_from_order_values() {
        let ol1 = Orderline{item_num: 10, price: 5};
        let ol2 = Orderline{item_num: 16, price: 32};
        let ol3 = Orderline{item_num: 20, price: 64};
        let order = Order::new(vec![ol1.clone(), ol2.clone(), ol3.clone()], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, _) = create_mutation_from_order(&order);
        let mut mutations = bmut.mutations.unwrap();
        let ol3_mut = mutations.pop().unwrap();
        let ol2_mut = mutations.pop().unwrap();
        let ol1_mut   = mutations.pop().unwrap();

        let exp_val: Vec<u8> = tuple_to_u8_vec((&ol3.item_num.to_string(), &ol3.price.to_string()));
        assert_eq!(ol3_mut.value.unwrap(), exp_val, "Orderline3 value did not match expected value");
        let exp_val: Vec<u8> = tuple_to_u8_vec((&ol2.item_num.to_string(), &ol2.price.to_string()));
        assert_eq!(ol2_mut.value.unwrap(), exp_val, "Orderline2 value did not match expected value");
        let exp_val: Vec<u8> = tuple_to_u8_vec((&ol1.item_num.to_string(), &ol1.price.to_string()));
        assert_eq!(ol1_mut.value.unwrap(), exp_val, "Orderline1 value did not match expected value");
    }

    #[test]
    fn test_create_mutation_from_order_columns() {
        let ol1 = Orderline{item_num: 10, price: 5};
        let ol2 = Orderline{item_num: 16, price: 32};
        let ol3 = Orderline{item_num: 20, price: 64};
        let order = Order::new(vec![ol1.clone(), ol2.clone(), ol3.clone()], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, _) = create_mutation_from_order(&order);
        let mut mutations = bmut.mutations.unwrap();
        let ol3_mut = mutations.pop().unwrap();
        let ol2_mut = mutations.pop().unwrap();
        let ol1_mut   = mutations.pop().unwrap();

        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ol", "2"));
        assert_eq!(ol3_mut.column.unwrap(), exp_cols, "Column family or Column for orderline3 did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ol", "1"));
        assert_eq!(ol2_mut.column.unwrap(), exp_cols, "Column family or Column for orderline2 did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ol", "0"));
        assert_eq!(ol1_mut.column.unwrap(), exp_cols, "Column family or Column for orderline1 did not match the expected names.");
    }

    #[test]
    fn test_create_mutation_from_empty_order_columns() {
        let order = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, _) = create_mutation_from_order(&order);
        let mut mutations = bmut.mutations.unwrap();
        let raddr_mut = mutations.pop().unwrap();
        let caddr_mut = mutations.pop().unwrap();
        let rid_mut   = mutations.pop().unwrap();
        let cid_mut   = mutations.pop().unwrap();
        let state_mut = mutations.pop().unwrap();
        let otime_mut = mutations.pop().unwrap();
        let o_id_mut  = mutations.pop().unwrap();
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("addr", "r_addr"));
        assert_eq!(raddr_mut.column.unwrap(), exp_cols, "Column family or Column for restaurant address did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("addr", "c_addr"));
        assert_eq!(caddr_mut.column.unwrap(), exp_cols, "Column family or Column for customer address did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ids", "r_id"));
        assert_eq!(rid_mut.column.unwrap(), exp_cols, "Column family or Column for restaurant id did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("ids", "c_id"));
        assert_eq!(cid_mut.column.unwrap(), exp_cols, "Column family or Column for customer id did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("info", "state"));
        assert_eq!(state_mut.column.unwrap(), exp_cols, "Column family or Column for state did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("info", "o_time"));
        assert_eq!(otime_mut.column.unwrap(), exp_cols, "Column family or Column for ordertime did not match the expected names.");
        let exp_cols: Vec<u8> = tuple_to_u8_vec(("info", "o_id"));
        assert_eq!(o_id_mut.column.unwrap(), exp_cols, "Column family or Column for order id did not match the expected names.");
    }

    #[test]
    fn test_create_mutation_from_empty_order_values() {
        let order = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, _) = create_mutation_from_order(&order);
        let mut mutations = bmut.mutations.unwrap();
        let raddr_mut = mutations.pop().unwrap();
        let caddr_mut = mutations.pop().unwrap();
        let rid_mut   = mutations.pop().unwrap();
        let cid_mut   = mutations.pop().unwrap();
        let state_mut = mutations.pop().unwrap();
        let otime_mut = mutations.pop().unwrap();
        let o_id_mut  = mutations.pop().unwrap();
        let exp_raddr: Vec<u8> = order.rest_addr.into();
        assert_eq!(raddr_mut.value.unwrap(), exp_raddr, "Restaurant Address did not match the expected address.");
        let exp_caddr: Vec<u8> = order.cust_addr.into();
        assert_eq!(caddr_mut.value.unwrap(), exp_caddr, "Customer Address did not match the expected address.");
        let exp_rid: Vec<u8> = order.r_id.into();
        assert_eq!(rid_mut.value.unwrap(), exp_rid, "Restaurant ID did not match the expected ID.");
        let exp_cid: Vec<u8> = order.c_id.into();
        assert_eq!(cid_mut.value.unwrap(), exp_cid, "Customer ID did not match the expected ID.");
        let exp_state: Vec<u8> = order.state.to_string().into();
        assert_eq!(state_mut.value.unwrap(), exp_state, "State did not match the expected State.");
        let exp_otime: Vec<u8> = order.ordertime.to_string().into();
        assert_eq!(otime_mut.value.unwrap(), exp_otime, "Ordertime did not match the expected Ordertime.");
        let exp_o_id: Vec<u8> = order.o_id.to_string().into();
        assert_eq!(o_id_mut.value.unwrap(), exp_o_id, "OrderId did not match the expected OrderId.");
    }

    #[test]
    fn test_create_mutation_from_order_not_empty() {
        let order = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, _) = create_mutation_from_order(&order);
        assert!(bmut.mutations.is_some());
    }

    #[test]
    fn test_create_mutation_from_order_row_key() {
        let order = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (_, rkey) = create_mutation_from_order(&order);
        let exp_rowkey = generate_row_key(&order);
        assert_eq!(rkey, exp_rowkey, "Wrong row key was set");
    }

    #[test]
    fn test_create_mutation_from_order_row_key_returned() {
        let order = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, rkey) = create_mutation_from_order(&order);
        let rkey: Vec<u8> = rkey.into();
        assert_eq!(bmut.row.unwrap(), rkey, "Returned wrong rkey");
    }

    #[test]
    fn test_create_cell_mutation_is_some() {
        let colfam = "columnfamily";
        let col = "col";
        let value = "value";
        let res = create_cell_mutation(colfam, col, value);
        assert!(res.column.is_some(), "Column was none");
        assert!(res.value.is_some(), "Value was none");
    }

    #[test]
    fn test_create_cell_mutation_correct() {
        let colfam = "columnfamily";
        let col = "col";
        let value = "value";
        let res = create_cell_mutation(colfam, col, value);
        let (res_colfam, res_col) = res.column.unwrap();
        let res_value = res.value.unwrap();
        let exp_value: Vec<u8> = value.into();
        assert_eq!(res_colfam, colfam, "Column Family did not match");
        assert_eq!(res_col, col, "Column name did not match");
        assert_eq!(res_value, exp_value, "Value did not match");
    }

    #[test]
    fn test_generate_salt_same_seed() {
        let inputseed = "Buddingevej 260, 2860 Soborg";
        let first = generate_salt(inputseed);
        let second = generate_salt(inputseed);
        assert_eq!(first, second, "Output changed between first and second salt generation");
    }

    #[test]
    fn test_generate_salt_different_seed() {
        let first = generate_salt("Buddingevej 260, 2860 Soborg");
        let second = generate_salt("Espegårdsvej 20, 2880 Bagsværd");
        assert_ne!(first, second, "Output was the same with both salt generations");
    }

    #[test]
    fn test_generate_salt_single_character_difference() {
        let first = generate_salt("Buddingevej 260, 2860 Soborg");
        let second = generate_salt("Buddingevej 260, 2860 Sobore");
        assert_ne!(first, second, "Output was the same with both salt generations");
    }

    #[test]
    fn test_generate_row_key_different_cust_rest() {
        let order1 = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let order2 = Order::new(Vec::new(), "addr".into(), "addr2".into(), "diffcustid".into(), "diffrestid".into());
        let rkey1 = generate_row_key(&order1);
        let rkey2 = generate_row_key(&order2);
        assert_ne!(rkey1, rkey2, "Row key was the same");
    }

    #[test]
    fn test_generate_row_key_same() {
        let order1 = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let order2 = order1.clone();
        let rkey1 = generate_row_key(&order1);
        let rkey2 = generate_row_key(&order2);
        assert_eq!(rkey1, rkey2, "Row key was generated differently with same input");
    }

    #[test]
    fn test_generate_row_key_front_same() {
        let restid = "restid".to_string();
        let order1 = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), restid.clone());
        let front = generate_salt(&restid);
        let rkey1 = generate_row_key(&order1);
        assert_eq!(rkey1[0..front.len()], front, "salt was not appended to front.");
    }

    fn tuple_to_u8_vec(tuple: (&str, &str)) -> Vec<u8> {
        format!("{}:{}", tuple.0, tuple.1).into()
    }
}