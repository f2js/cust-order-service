use std::collections::BTreeMap;

use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel, WriteHalf, ReadHalf},
};

use hbase_thrift::{hbase::{HbaseSyncClient, Text, THbaseSyncClient, BatchMutation, ColumnDescriptor, TRowResult, ScannerID, TScan}, THbaseSyncClientExt, Attributes};

use crate::models::errors::OrderServiceError;

#[cfg_attr(test, mockall::automock)]
pub trait HbaseClient {
    fn get_table_names(&mut self) -> Result<Vec<Text>, OrderServiceError>;
    fn put(
        &mut self,
        table_name: &str,
        row_batches: Vec<BatchMutation>,
        timestamp: Option<i64>,
        attributes: Option<Attributes>,
    ) -> thrift::Result<()>;
    fn create_table(&mut self, table_name: &str, column_families: Vec<String>) -> Result<(), OrderServiceError>;
    fn get_row(&mut self, row_id: &str) -> Result<Vec<TRowResult>, OrderServiceError>;
    fn scanner_open_with_scan(&mut self, table_name: Text, scan: TScan, attributes: BTreeMap<Text, Text>) -> Result<ScannerID, OrderServiceError>;
    fn scanner_get_list(&mut self, id: ScannerID, nb_rows: i32) -> Result<Vec<TRowResult>, OrderServiceError>;
}

pub struct HbaseConnection {
    connection: HbaseSyncClient<TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>, TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>>,
}

impl HbaseConnection {
    pub fn connect(url: &str) -> Result<Self, OrderServiceError> {
        let (i_prot, o_prot) = get_protocols(&url)?;
        Ok(Self{
            connection: HbaseSyncClient::new(i_prot, o_prot)
        })
    }    
}

impl HbaseClient for HbaseConnection {
    fn get_table_names(&mut self) -> Result<Vec<Text>, OrderServiceError> {
        match self.connection.get_table_names() {
            Ok(r) => Ok(r),
            Err(e) => Err(OrderServiceError::DBError(e)),
        }
    }

    fn put(
        &mut self,
        table_name: &str,
        row_batches: Vec<BatchMutation>,
        timestamp: Option<i64>,
        attributes: Option<Attributes>,
    ) -> thrift::Result<()> {
        self.connection.put(&table_name, row_batches, timestamp, attributes)
    } 
    fn create_table(&mut self, table_name: &str, column_families: Vec<String>) -> Result<(), OrderServiceError> {
        match self.connection.table_exists(table_name) {
            Ok(r) => if r {return Ok(())},
            Err(e) => return Err(OrderServiceError::from(e)),
        };
        let colfams: Vec<ColumnDescriptor> = column_families.iter().map(|elem| {
            ColumnDescriptor {
                name: Some(elem.to_owned().into()),
                compression: Some("NONE".into()),
                time_to_live: Some(0x7fffffff),
                max_versions: Some(3),
                bloom_filter_type: Some("NONE".into()),
                ..Default::default()
            }
        }).collect();
        match self.connection.create_table(table_name.into(), colfams) {
            Ok(_) => Ok(()),
            Err(e) => Err(OrderServiceError::DBError(e)),
        }
    }
    fn get_row(&mut self, row_id: &str) -> Result<Vec<TRowResult>, OrderServiceError> {
        match self.connection.get_row("orders".into(), row_id.into(), BTreeMap::default()) {
            Ok(r) => Ok(r),
            Err(e) => Err(OrderServiceError::DBError(e)),
        }
    }
    fn scanner_open_with_scan(&mut self, table_name: Text, scan: TScan, attributes: BTreeMap<Text, Text>) -> Result<ScannerID, OrderServiceError> {
        match self.connection.scanner_open_with_scan(table_name, scan, attributes) {
            Ok(r) => Ok(r),
            Err(e) => Err(OrderServiceError::DBError(e)),
        }
    }
    fn scanner_get_list(&mut self,id:ScannerID,nb_rows:i32) -> Result<Vec<TRowResult>, OrderServiceError> {
        match self.connection.scanner_get_list(id, nb_rows) {
            Ok(r) => Ok(r),
            Err(e) => Err(OrderServiceError::DBError(e)),
        }
    }
    
}

fn get_protocols(url: &str) -> Result<(TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>, TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>), thrift::Error> {
    let mut channel = TTcpChannel::new();
    channel.open(url)?;
    let (i_chan, o_chan) = channel.split()?;

    let i_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(i_chan), true);
    let o_prot = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(o_chan), true);

    Ok((i_prot, o_prot))
}