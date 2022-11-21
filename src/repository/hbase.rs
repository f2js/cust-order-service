use hbase_thrift::{hbase::THbaseSyncClient, THbaseSyncClientExt};

use crate::models::{tables::TableName, orders::Order};
use crate::repository::hbase_utils::{connect_client, create_mutation_from_order};

pub fn get_tables() -> Result<Vec<TableName>, thrift::Error> {
    let mut client = connect_client()?;

    let tables = client.get_table_names()?;
    let tables_names = tables
        .into_iter()
        .map(|v| TableName::new(String::from_utf8(v).unwrap()))
        .collect::<Vec<_>>();
    Ok(tables_names)
}

pub fn add_order(order:Order) -> Result<String, thrift::Error> {
    let mut client = connect_client()?;

    let (batch, rowkey) = create_mutation_from_order(&order);
    match client.put("orders", vec![batch], None, None) {
        Ok(_) => Ok(rowkey),
        Err(e) => Err(e),
    }
}