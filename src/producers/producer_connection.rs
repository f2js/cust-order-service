use kafka::{producer::{Producer, Record, RequiredAcks, AsBytes}};
use crate::models::{orders::Order, errors::OrderServiceError};

#[cfg_attr(test, mockall::automock)]
pub trait KafkaProducer {
    fn send(&mut self, topic: &str, json: String) -> Result<(), OrderServiceError>;
}

pub struct KafkaProdConnection {
    con: Producer
}

impl KafkaProducer for KafkaProdConnection {
    fn send(&mut self, topic: &str, json: String) -> Result<(), OrderServiceError> {
        match self.con.send(&Record::from_value(topic, json.as_bytes())) {
            Ok(r) => Ok(r),
            Err(e) => Err(OrderServiceError::EventBrokerError(e)),
        }
    }
}

impl KafkaProdConnection {
    pub fn connect(kafka_ip: String) -> Result<Self, OrderServiceError> { 
        let con = Producer::from_hosts(vec!(kafka_ip))
            .with_ack_timeout(std::time::Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()?;
        Ok(Self {
            con
        })
    }
}