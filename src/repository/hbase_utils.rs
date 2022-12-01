use hbase_thrift::{hbase::{BatchMutation, TScan}, MutationBuilder, BatchMutationBuilder};

use rand::prelude::*;
use rand_seeder::{Seeder};
use rand_pcg::Pcg64;

use crate::models::{orders::{Order, Orderline, OrderBuilder}};

pub(crate) fn create_mutation_from_order(order: &Order) -> (BatchMutation, String) {
    //let id_mut = create_cell_mutation("info", "o_id", order.o_id.to_string());
    let otime_mut = create_cell_mutation("info", "o_time", order.ordertime.to_string());
    let state_mut = create_cell_mutation("info", "state", order.state.to_string());
    let cid_mut = create_cell_mutation("ids", "c_id", order.c_id.clone());
    let rid_mut = create_cell_mutation("ids", "r_id", order.r_id.clone());
    let caddr_mut = create_cell_mutation("addr", "c_addr", order.cust_addr.clone());
    let raddr_mut = create_cell_mutation("addr", "r_addr", order.rest_addr.clone());
    
    let mut mutations = vec![otime_mut, state_mut, cid_mut, rid_mut, caddr_mut, raddr_mut];
    for (i, orderline) in order.orderlines.iter().enumerate() {
        let orderline = create_cell_mutation("ol", i.to_string(), format!("{:?}:{:?}", orderline.item_num, orderline.price));
        mutations.push(orderline);
    }
    let rowkey = order.o_id.clone();
    (<BatchMutationBuilder>::default().row(rowkey.clone()).mutations(mutations).build(), rowkey)
}


fn create_cell_mutation(column_family: impl Into<String>, column: impl Into<String>,  value: impl Into<Vec<u8>>) -> MutationBuilder {
    let mut mutation = MutationBuilder::default();
    mutation.column(column_family, column);
    mutation.value(value);
    mutation
}

pub fn create_order_builder_from_hbase_row(
    hbase_row: &hbase_thrift::hbase::TRowResult,
) -> OrderBuilder {
    let mut order_builder = OrderBuilder::default();
    let cols = match &hbase_row.columns{
        Some(v) =>v,
        None => return order_builder,
    };
    for (col, cell) in cols.iter() {
        let col = col.clone();
        let cell = cell.clone().value;
        let (column, value) = match get_column_and_value(col, cell) {
            Some(v) => v,
            None => continue,
        };
        set_order_field(column, value, &mut order_builder);
    }
    order_builder
}

fn get_column(col: Vec<u8>) -> Option<(String, String)> {
    let column: String = match std::str::from_utf8(&col) {
        Ok(colname) => colname.to_string(),
        Err(_) => return None,
    };
    Some(match column.split_once(':') {
        Some(v) => (v.0.to_owned(), v.1.to_owned()),
        None => return None,
    })
}

fn get_value(cell: Option<Vec<u8>>) -> Option<String> {
    let cell = cell?;
    Some(match std::str::from_utf8(&cell) {
        Ok(val) => val.to_string(),
        Err(_) => return None,
    })
}

fn get_column_and_value(col: Vec<u8>, cell: Option<Vec<u8>>) -> Option<((String, String),String)> {
    Some((get_column(col)?, get_value(cell)?))
}

fn set_order_field(field: (String, String), val: String, order_builder: &mut OrderBuilder) {
    let col: (&str, &str) = (&field.0, &field.1);
    match col {
        ("info", "o_id") => order_builder.o_id = Some(val.clone()),
        ("info", "o_time") => order_builder.ordertime = Some(val.clone()),
        ("info", "state") => order_builder.state = Some(val.clone()),
        ("ids", "c_id") => order_builder.c_id = Some(val.clone()),
        ("ids", "r_id") => order_builder.r_id = Some(val.clone()),
        ("addr", "c_addr") => order_builder.cust_addr = Some(val.clone()),
        ("addr", "r_addr") => order_builder.rest_addr = Some(val.clone()),
        ("ol", _) => {
            let result = <Orderline as std::str::FromStr>::from_str(&val.clone());
            match result {
                Ok(ol) => order_builder.orderlines.push(ol),
                Err(_) => println!("Badly formatted Orderline."),
            }
        }
        (_, _) => println!("Unknown column type"),
    }
}

pub fn build_single_column_filter(colfam: &str, col: &str, operator: &str, value: &str) -> String {
    format!("SingleColumnValueFilter('{colfam}', '{col}', {operator}, 'binaryprefix:{value}')")
}

pub fn create_scan(columns_to_fetch: Vec<Vec<u8>>, filter_colfam: &str, filter_col: &str, filter_val: &str) -> TScan {
    TScan {
        columns: Some(columns_to_fetch),
        filter_string: Some(build_single_column_filter(filter_colfam, filter_col, "=", &filter_val).into()),
        start_row: None,
        stop_row: None,
        timestamp: None,
        caching: None,
        batch_size: Some(0),
        sort_columns: Some(false),
        reversed: Some(false),
        cache_blocks: Some(false),
    }
}

// Only for testing purposes 
pub(crate) fn order_to_trowresult(order: Order) -> hbase_thrift::hbase::TRowResult {
    let mut columns: std::collections::BTreeMap<hbase_thrift::hbase::Text, hbase_thrift::hbase::TCell> = std::collections::BTreeMap::new();
    columns.insert("info:o_id".as_bytes().to_vec(), _to_tcell(&order.o_id));
    columns.insert("info:o_time".as_bytes().to_vec(), _to_tcell(&order.ordertime));
    columns.insert("info:state".as_bytes().to_vec(), _to_tcell(&order.state.to_string()));
    columns.insert("ids:c_id".as_bytes().to_vec(), _to_tcell(&order.c_id));
    columns.insert("ids:r_id".as_bytes().to_vec(), _to_tcell(&order.r_id));
    columns.insert("addr:c_addr".as_bytes().to_vec(), _to_tcell(&order.cust_addr));
    columns.insert("addr:r_addr".as_bytes().to_vec(), _to_tcell(&order.rest_addr));
    for (i, v) in order.orderlines.iter().enumerate() {
        columns.insert(format!("ol:{i}").as_bytes().to_vec(), _to_tcell(&v.to_string()));
    };
    hbase_thrift::hbase::TRowResult { row: Some("row".as_bytes().to_vec()), columns: Some(columns), sorted_columns: None }
}

fn _to_tcell(val: &str) -> hbase_thrift::hbase::TCell {
    hbase_thrift::hbase::TCell { value: Some(val.as_bytes().to_vec()), timestamp: Some(0) }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr};

    use super::*;
    use crate::models::orders::{Order, Orderline, OrderBuilder};

    #[test]
    fn test_create_order_builder_from_hbase_row_unknown_field() {
        let order = Order::new(vec![], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let mut columns: std::collections::BTreeMap<hbase_thrift::hbase::Text, hbase_thrift::hbase::TCell> = std::collections::BTreeMap::new();
        columns.insert("inaaafo:oooo_id".as_bytes().to_vec(), _to_tcell(&order.o_id));
        columns.insert("info:o_time".as_bytes().to_vec(), _to_tcell(&order.ordertime));
        columns.insert("info:state".as_bytes().to_vec(), _to_tcell(&order.state.to_string()));
        columns.insert("ids:c_id".as_bytes().to_vec(), _to_tcell(&order.c_id));
        columns.insert("ids:r_id".as_bytes().to_vec(), _to_tcell(&order.r_id));
        columns.insert("addr:c_addr".as_bytes().to_vec(), _to_tcell(&order.cust_addr));
        columns.insert("addr:r_addr".as_bytes().to_vec(), _to_tcell(&order.rest_addr));
        let trowresult = hbase_thrift::hbase::TRowResult { row: Some("row".as_bytes().to_vec()), columns: Some(columns), sorted_columns: None };
        let obuilder = create_order_builder_from_hbase_row(&trowresult);
        assert!(obuilder.o_id.is_none());

        assert!(obuilder.c_id.is_some());
        assert!(obuilder.r_id.is_some());
        assert!(obuilder.ordertime.is_some());
        assert!(obuilder.cust_addr.is_some());
        assert!(obuilder.rest_addr.is_some());
        assert!(obuilder.state.is_some());
        assert!(obuilder.orderlines.len() == 0)
    }

    #[test]
    fn test_create_order_builder_from_hbase_row_missing_field() {
        let order = Order::new(vec![], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let mut columns: std::collections::BTreeMap<hbase_thrift::hbase::Text, hbase_thrift::hbase::TCell> = std::collections::BTreeMap::new();
        columns.insert("info:o_id".as_bytes().to_vec(), _to_tcell(&order.o_id));
        columns.insert("info:o_time".as_bytes().to_vec(), _to_tcell(&order.ordertime));
        columns.insert("info:state".as_bytes().to_vec(), _to_tcell(&order.state.to_string()));
        // columns.insert("ids:c_id".as_bytes().to_vec(), _to_tcell(&order.c_id));
        columns.insert("ids:r_id".as_bytes().to_vec(), _to_tcell(&order.r_id));
        columns.insert("addr:c_addr".as_bytes().to_vec(), _to_tcell(&order.cust_addr));
        columns.insert("addr:r_addr".as_bytes().to_vec(), _to_tcell(&order.rest_addr));
        let trowresult = hbase_thrift::hbase::TRowResult { row: Some("row".as_bytes().to_vec()), columns: Some(columns), sorted_columns: None };
        let obuilder = create_order_builder_from_hbase_row(&trowresult);
        assert!(obuilder.c_id.is_none());

        assert!(obuilder.o_id.is_some());
        assert!(obuilder.r_id.is_some());
        assert!(obuilder.ordertime.is_some());
        assert!(obuilder.cust_addr.is_some());
        assert!(obuilder.rest_addr.is_some());
        assert!(obuilder.state.is_some());
        assert!(obuilder.orderlines.len() == 0)
    }

    #[test]
    fn test_create_order_builder_from_hbase_row_on_content() {
        let ol1 = Orderline{item_num: 10, price: 5};
        let ol2 = Orderline{item_num: 16, price: 32};
        let ol3 = Orderline{item_num: 20, price: 64};
        let order = Order::new(vec![ol1.clone(), ol2.clone(), ol3.clone()], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let trowresult = order_to_trowresult(order.clone());
        let obuilder = create_order_builder_from_hbase_row(&trowresult);
        assert_eq!(obuilder.o_id.unwrap(), order.o_id);
        assert_eq!(obuilder.r_id.unwrap(), order.r_id);
        assert_eq!(obuilder.c_id.unwrap(), order.c_id);
        assert_eq!(obuilder.cust_addr.unwrap(), order.cust_addr);
        assert_eq!(obuilder.rest_addr.unwrap(), order.rest_addr);
        assert_eq!(obuilder.state.unwrap(), order.state.to_string());
        assert_eq!(obuilder.ordertime.unwrap(), order.ordertime);
        assert!(obuilder.orderlines.len() == 3);
        assert_eq!(obuilder.orderlines[0], ol1);
        assert_eq!(obuilder.orderlines[1], ol2);
        assert_eq!(obuilder.orderlines[2], ol3);
    }

    #[test]
    fn test_create_order_builder_from_hbase_row_on_content_empty_order() {
        let order = Order::new(vec![], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let trowresult = order_to_trowresult(order.clone());
        let obuilder = create_order_builder_from_hbase_row(&trowresult);
        assert_eq!(obuilder.o_id.unwrap(), order.o_id);
        assert_eq!(obuilder.r_id.unwrap(), order.r_id);
        assert_eq!(obuilder.c_id.unwrap(), order.c_id);
        assert_eq!(obuilder.cust_addr.unwrap(), order.cust_addr);
        assert_eq!(obuilder.rest_addr.unwrap(), order.rest_addr);
        assert_eq!(obuilder.state.unwrap(), order.state.to_string());
        assert_eq!(obuilder.ordertime.unwrap(), order.ordertime);
        assert!(obuilder.orderlines.len() == 0);
    }

    #[test]
    fn test_create_order_builder_from_hbase_row_is_some() {
        let order = Order::new(vec![], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let trowresult = order_to_trowresult(order);
        let obuilder = create_order_builder_from_hbase_row(&trowresult);
        assert!(obuilder.o_id.is_some());
        assert!(obuilder.c_id.is_some());
        assert!(obuilder.r_id.is_some());
        assert!(obuilder.ordertime.is_some());
        assert!(obuilder.cust_addr.is_some());
        assert!(obuilder.rest_addr.is_some());
        assert!(obuilder.state.is_some());
        assert!(obuilder.orderlines.len() == 0)
    }

    #[test]
    fn test_create_scan_with_cols() {
        let cols = vec!["col1:col".into(), "col2".into()];
        let colfam = "testcolfam";
        let col = "testcol";
        let val = "testval";
        let scan = create_scan(cols.clone(), colfam, col, val);
        assert_eq!(scan.columns.unwrap(), cols);
        assert_eq!(scan.filter_string.unwrap(), Into::<Vec<u8>>::into(build_single_column_filter(colfam, col, "=", val)));
    }

    #[test]
    fn test_create_scan_no_cols() {
        let cols = vec![];
        let colfam = "testcolfam";
        let col = "testcol";
        let val = "testval";
        let scan = create_scan(cols.clone(), colfam, col, val);
        assert_eq!(scan.columns.unwrap(), cols);
        assert_eq!(scan.filter_string.unwrap(), Into::<Vec<u8>>::into(build_single_column_filter(colfam, col, "=", val)));
    }

    #[test]
    fn test_build_single_column_filter() {
        let exp = "SingleColumnValueFilter('test', 'test', =, 'binaryprefix:test')";
        let actual = build_single_column_filter("test", "test", "=", "test");
        assert_eq!(actual, exp.to_string());
    }

    #[test]
    fn test_get_column_bad_str() {
        let input:Vec<u8> = vec![255,255,58,255,255];
        let actual = get_column(input);
        assert!(actual.is_none());
    }

    #[test]
    fn test_get_column_bad_split() {
        let input:Vec<u8> = "colfamcol".into();
        let actual = get_column(input);
        assert!(actual.is_none());
    }

    #[test]
    fn test_get_column() {
        let expected = ("colfam", "col");
        let input:Vec<u8> = "colfam:col".into();
        let actual = get_column(input);
        assert!(actual.is_some());
        let actual = actual.unwrap();
        assert_eq!(actual.0, expected.0);
        assert_eq!(actual.1, expected.1);
    }

    #[test]
    fn test_get_value_none() {
        let actual = get_value(None);
        assert!(actual.is_none());
    }

    #[test]
    fn test_get_value_bad_str() {
        let input: Vec<u8> = vec![255,255,255,255];
        let actual = get_value(Some(input));
        assert!(actual.is_none());
    }

    #[test]
    fn test_get_value() {
        let expected = "hello, world";
        let input: Vec<u8> = expected.into();
        let actual = get_value(Some(input));
        assert!(actual.is_some());
        assert_eq!(actual.unwrap(), expected);
    }

    #[test]
    fn test_set_order_field_bad_col() {
        let field = ("addr".to_string(), "o_id".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.o_id.is_none());
        assert!(order_builder.state.is_none());
        assert!(order_builder.ordertime.is_none());
        assert!(order_builder.r_id.is_none());
        assert!(order_builder.c_id.is_none());
        assert!(order_builder.rest_addr.is_none());
        assert!(order_builder.cust_addr.is_none());
        assert!(order_builder.orderlines.len() == 0);
    }

    #[test]
    fn test_set_order_field_bad_colfam() {
        let field = ("aaaaadr".to_string(), "r_addr".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.o_id.is_none());
        assert!(order_builder.state.is_none());
        assert!(order_builder.ordertime.is_none());
        assert!(order_builder.r_id.is_none());
        assert!(order_builder.c_id.is_none());
        assert!(order_builder.rest_addr.is_none());
        assert!(order_builder.cust_addr.is_none());
        assert!(order_builder.orderlines.len() == 0);
    }

    #[test]
    fn test_set_order_field_bad_and_good_ol() {
        let field = ("ol".to_string(), "1".to_string());
        let val1 = "12:12".to_string();
        let val2 = "256".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field.clone(), val1.clone(), &mut order_builder);
        set_order_field(field, val2.clone(), &mut order_builder);
        let exp1 = Orderline::from_str(&val1).unwrap();
        assert!(order_builder.orderlines.len() == 1);
        assert_eq!(order_builder.orderlines[0], exp1);
    }

    #[test]
    fn test_set_order_field_two_ol() {
        let field = ("ol".to_string(), "1".to_string());
        let val1 = "12:12".to_string();
        let val2 = "16:24".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field.clone(), val1.clone(), &mut order_builder);
        set_order_field(field, val2.clone(), &mut order_builder);
        let exp1 = Orderline::from_str(&val1).unwrap();
        let exp2 = Orderline::from_str(&val2).unwrap();
        assert!(order_builder.orderlines.len() == 2);
        assert_eq!(order_builder.orderlines[0], exp1);
        assert_eq!(order_builder.orderlines[1], exp2);
    }

    #[test]
    fn test_set_order_field_bad_ol() {
        let field = ("ol".to_string(), "1".to_string());
        let val = "hej".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.orderlines.len() == 0);
    }

    #[test]
    fn test_set_order_field_ol() {
        let field = ("ol".to_string(), "1".to_string());
        let val = "12:12".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.orderlines.len() == 1);
        let exp = Orderline::from_str(&val).unwrap();
        assert_eq!(order_builder.orderlines[0], exp);
    }

    #[test]
    fn test_set_order_field_r_addr() {
        let field = ("addr".to_string(), "r_addr".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.rest_addr.is_some());
        assert_eq!(order_builder.rest_addr.unwrap(), val);
    }

    #[test]
    fn test_set_order_field_c_addr() {
        let field = ("addr".to_string(), "c_addr".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.cust_addr.is_some());
        assert_eq!(order_builder.cust_addr.unwrap(), val);
    }

    #[test]
    fn test_set_order_field_r_id() {
        let field = ("ids".to_string(), "r_id".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.r_id.is_some());
        assert_eq!(order_builder.r_id.unwrap(), val);
    }

    #[test]
    fn test_set_order_field_c_id() {
        let field = ("ids".to_string(), "c_id".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.c_id.is_some());
        assert_eq!(order_builder.c_id.unwrap(), val);
    }

    #[test]
    fn test_set_order_field_state() {
        let field = ("info".to_string(), "state".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.state.is_some());
        assert_eq!(order_builder.state.unwrap(), val);
    }

    #[test]
    fn test_set_order_field_o_id() {
        let field = ("info".to_string(), "o_id".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.o_id.is_some());
        assert_eq!(order_builder.o_id.unwrap(), val);
    }

    #[test]
    fn test_set_order_field_o_time() {
        let field = ("info".to_string(), "o_time".to_string());
        let val = "value".to_string();
        let mut order_builder = OrderBuilder::default();
        set_order_field(field, val.clone(), &mut order_builder);
        assert!(order_builder.ordertime.is_some());
        assert_eq!(order_builder.ordertime.unwrap(), val);
    }

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
    }

    #[test]
    fn test_create_mutation_from_order_full_values() {
        let ol1 = Orderline{item_num: 10, price: 5};
        let ol2 = Orderline{item_num: 16, price: 32};
        let ol3 = Orderline{item_num: 20, price: 64};
        let order = Order::new(vec![ol1.clone(), ol2.clone(), ol3.clone()], "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, o_id) = create_mutation_from_order(&order);
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
        assert_eq!(Into::<Vec<u8>>::into(o_id), exp_o_id, "OrderId did not match the expected OrderId.");
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
    }

    #[test]
    fn test_create_mutation_from_empty_order_values() {
        let order = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, o_id) = create_mutation_from_order(&order);
        let mut mutations = bmut.mutations.unwrap();
        let raddr_mut = mutations.pop().unwrap();
        let caddr_mut = mutations.pop().unwrap();
        let rid_mut   = mutations.pop().unwrap();
        let cid_mut   = mutations.pop().unwrap();
        let state_mut = mutations.pop().unwrap();
        let otime_mut = mutations.pop().unwrap();
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
        assert_eq!(Into::<Vec<u8>>::into(o_id), exp_o_id, "OrderId did not match the expected OrderId.");
    }

    #[test]
    fn test_create_mutation_from_order_not_empty() {
        let order = Order::new(Vec::new(), "addr".into(), "addr2".into(), "custid".into(), "restid".into());
        let (bmut, _) = create_mutation_from_order(&order);
        assert!(bmut.mutations.is_some());
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

    fn tuple_to_u8_vec(tuple: (&str, &str)) -> Vec<u8> {
        format!("{}:{}", tuple.0, tuple.1).into()
    }
}