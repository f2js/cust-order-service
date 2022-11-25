use crate::models::{orders::Order, tables::TableName};
use crate::repository::hbase_connection::HbaseClient;
use crate::repository::hbase_utils::{create_mutation_from_order, try_create_order_from_hbase_row};

pub fn get_tables(mut client: impl HbaseClient) -> Result<Vec<TableName>, thrift::Error> {
    let tables = client.get_table_names()?;
    let tables_names = tables
        .into_iter()
        .map(|v| TableName::new(String::from_utf8(v).unwrap()))
        .collect::<Vec<_>>();
    Ok(tables_names)
}

pub fn add_order(order: Order, mut client: impl HbaseClient) -> Result<String, thrift::Error> {
    let (batch, rowkey) = create_mutation_from_order(&order);
    match client.put("orders", vec![batch], None, None) {
        Ok(_) => Ok(rowkey),
        Err(e) => Err(e),
    }
}

pub fn create_order_table(mut client: impl HbaseClient) -> Result<(), thrift::Error> {
    match client.create_table(
        "orders",
        vec!["info".into(), "ids".into(), "addr".into(), "ol".into()],
    ) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub fn get_order_row(row_id: &str, mut client: impl HbaseClient) -> Result<Order, thrift::Error> {
    let r = client.get_row(row_id)?;
    let row = match r.get(0) {
        Some(v) => v,
        None => return Err(thrift::Error::User("Error, No content found".into())),
    };
    match try_create_order_from_hbase_row(row) {
        Some(v) => Ok(v),
        None => return Err(thrift::Error::User("Error reading row contents".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        models::orders::Orderline,
        repository::{hbase_connection::MockHbaseClient, hbase_utils::create_mutation_from_order},
    };
    use hbase_thrift::{
        hbase::{BatchMutation, Text},
        Attributes,
    };
    use mockall::predicate::eq;

    // #[test]
    // fn test_debugging() {
    //     const DB_IP: &str = "165.22.194.124:9090";
    //     let con = crate::repository::hbase_connection::HbaseConnection::connect(&DB_IP).unwrap();
    //     let r = get_order_row("2441910473035", con);
    //     let order = match r {
    //         Ok(r) => r,
    //         Err(e) => panic!("ERRROR"),
    //     };
    // }

    #[test]
    fn test_add_order_empty() {
        let order = Order::new(
            Vec::new(),
            "addr".into(),
            "addr2".into(),
            "custid".into(),
            "restid".into(),
        );
        let (mutations, rkey) = create_mutation_from_order(&order);
        let mut mock_con = MockHbaseClient::new();
        mock_con
            .expect_put()
            .withf(
                move |tblname: &str,
                      row_batches: &Vec<BatchMutation>,
                      timestamp: &Option<i64>,
                      attributes: &Option<Attributes>| {
                    tblname.eq("orders")
                        && *timestamp == Option::None
                        && *attributes == Option::None
                        && row_batches.eq(&vec![mutations.clone()])
                },
            )
            .times(1)
            .returning(move |_tblname, _batch, _tmstmp, _attr| Ok(()));
        let res = add_order(order, mock_con);
        assert_eq!(res.unwrap(), rkey);
    }

    #[test]
    fn test_add_order_with_contents() {
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
        let order = Order::new(
            vec![ol1, ol2, ol3],
            "addr".into(),
            "addr2".into(),
            "custid".into(),
            "restid".into(),
        );
        let (mutations, rkey) = create_mutation_from_order(&order);
        let mut mock_con = MockHbaseClient::new();
        mock_con
            .expect_put()
            .withf(
                move |tblname: &str,
                      row_batches: &Vec<BatchMutation>,
                      timestamp: &Option<i64>,
                      attributes: &Option<Attributes>| {
                    tblname.eq("orders")
                        && *timestamp == Option::None
                        && *attributes == Option::None
                        && row_batches.eq(&vec![mutations.clone()])
                },
            )
            .times(1)
            .returning(move |_tblname, _batch, _tmstmp, _attr| Ok(()));
        let res = add_order(order, mock_con);
        assert_eq!(res.unwrap(), rkey);
    }

    #[test]
    fn test_get_tables_single() {
        let exp = "orders";
        let mut mock_con = MockHbaseClient::new();
        mock_con
            .expect_get_table_names()
            .times(1)
            .returning(move || Ok(vec![Text::from(exp.clone())]));

        let res = get_tables(mock_con);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.len() == 1);
        let res = res.get(0).unwrap();
        assert_eq!(res.table_name, exp);
    }

    #[test]
    fn test_get_tables_multiple() {
        let exp1 = "orders";
        let exp2 = "other";
        let mut mock_con = MockHbaseClient::new();
        mock_con
            .expect_get_table_names()
            .times(1)
            .returning(move || Ok(vec![Text::from(exp1.clone()), Text::from(exp2.clone())]));

        let res = get_tables(mock_con);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.len() == 2);
        let res1 = res.get(0).unwrap();
        let res2 = res.get(1).unwrap();
        assert_eq!(res1.table_name, exp1);
        assert_eq!(res2.table_name, exp2);
    }

    #[test]
    fn test_get_tables_none() {
        let mut mock_con = MockHbaseClient::new();
        mock_con
            .expect_get_table_names()
            .times(1)
            .returning(move || Ok(vec![]));

        let res = get_tables(mock_con);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.len() == 0);
    }
}
