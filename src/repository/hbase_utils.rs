use hbase_thrift::{hbase::{BatchMutation, HbaseSyncClient}, MutationBuilder, BatchMutationBuilder};

use rand::prelude::*;
use rand_seeder::{Seeder};
use rand_pcg::Pcg64;
use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel, WriteHalf, ReadHalf},
};
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

pub(crate) fn connect_client() -> Result<HbaseSyncClient<TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>, TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>>, thrift::Error> {
    let (i_prot, o_prot) = get_protocols()?;
    Ok(HbaseSyncClient::new(i_prot, o_prot))
}

fn get_protocols() -> Result<(TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>, TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>), thrift::Error> {
    let mut channel = TTcpChannel::new();
    channel.open("165.22.194.124:9090")?;
    let (i_chan, o_chan) = channel.split()?;

    let i_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(i_chan), true);
    let o_prot = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(o_chan), true);

    Ok((i_prot, o_prot))
}

fn create_cell_mutation(column_family: impl Into<String>, column: impl Into<String>,  value: impl Into<Vec<u8>>) -> MutationBuilder {
    let mut mutation = MutationBuilder::default();
    mutation.column(column_family, column);
    mutation.value(value);
    mutation
}

fn generate_row_key(order: &Order) -> String {
    let mut res = String::from(generate_salt(&order.rest_addr));
    res.push_str(&order.o_id.to_string());
    res
}

fn generate_salt(seed: &str) -> String {
    let mut rng: Pcg64 = Seeder::from(seed).make_rng();
    rng.gen::<u8>().to_string()
}

#[cfg(test)]
mod tests {
    use super::generate_salt;
    #[test]
    fn test() {
        let res = generate_salt("Buddingevej 260, 2860 Soborg");
        println!("{:?}", res);
    }
}