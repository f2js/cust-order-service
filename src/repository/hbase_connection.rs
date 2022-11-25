use std::collections::BTreeMap;

use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel, WriteHalf, ReadHalf},
};

use hbase_thrift::{hbase::{HbaseSyncClient, Text, THbaseSyncClient, BatchMutation, ColumnDescriptor, TRowResult}, THbaseSyncClientExt, Attributes};

use mockall::{automock, predicate::*};

#[cfg_attr(test, automock)]
pub trait HbaseClient {
    fn get_table_names(&mut self) -> thrift::Result<Vec<Text>>;
    fn put(
        &mut self,
        table_name: &str,
        row_batches: Vec<BatchMutation>,
        timestamp: Option<i64>,
        attributes: Option<Attributes>,
    ) -> thrift::Result<()>;
    fn create_table(&mut self, table_name: &str, column_families: Vec<String>) -> Result<(), thrift::Error>;
    fn get_row(&mut self, row_id: &str) -> Result<Vec<TRowResult>, thrift::Error>;
}

pub struct HbaseConnection {
    connection: HbaseSyncClient<TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>, TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>>,
}

impl HbaseConnection {
    pub fn connect(url: &str) -> Result<Self, thrift::Error> {
        let (i_prot, o_prot) = get_protocols(&url)?;
        Ok(Self{
            connection: HbaseSyncClient::new(i_prot, o_prot)
        })
    }    
}

impl HbaseClient for HbaseConnection {
    fn get_table_names(&mut self) -> thrift::Result<Vec<Text>> {
        self.connection.get_table_names()
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
    fn create_table(&mut self, table_name: &str, column_families: Vec<String>) -> Result<(), thrift::Error> {
        match self.connection.table_exists(table_name) {
            Ok(r) => if r {return Ok(())},
            Err(e) => return Err(e),
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
        self.connection.create_table(table_name.into(), colfams)
    }
    fn get_row(&mut self, row_id: &str) -> Result<Vec<TRowResult>, thrift::Error> {
        self.connection.get_row("orders".into(), row_id.into(), BTreeMap::default())
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