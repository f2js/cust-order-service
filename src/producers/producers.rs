use crate::models::{errors::OrderServiceError, orders::Order};

use super::producer_connection::{KafkaProducer};

pub fn publish_order_created(order: Order, producer: &mut impl KafkaProducer) -> Result<(), OrderServiceError> {
    let json = order.to_json_string()?;
    producer.send("OrderCreated", json)
}

#[cfg(test)]
mod tests {
    use crate::producers::producer_connection::MockKafkaProducer;

    use super::*;

    #[test]
    fn test_raise_event_is_ok() {
        let order = Order::new(vec![], "CustAddr".into(), "RestAddr".into(), "custid".into(), "restid".into());
        let exp_json  = format!(
            "{{\"o_id\":\"{}\",\"c_id\":\"{}\",\"r_id\":\"{}\",\"ordertime\":\"{}\",\"orderlines\":[],\"state\":\"{}\",\"cust_addr\":\"{}\",\"rest_addr\":\"{}\"}}", 
            order.o_id, order.c_id, order.r_id, order.ordertime, order.state, order.cust_addr, order.rest_addr);
        let mut mock_prod = MockKafkaProducer::new();
        mock_prod.expect_send()
            .withf(move |x, y| {
                x.eq("OrderCreated") && y.eq(&exp_json)
            })
            .times(1)
            .returning(|_x, _y| {
                Ok(())
            });
        let res = publish_order_created(order, &mut mock_prod);
        assert!(res.is_ok());
    }

    #[test]
    fn test_raise_event_is_err() {
        let order = Order::new(vec![], "CustAddr".into(), "RestAddr".into(), "custid".into(), "restid".into());
        let exp_json  = format!(
            "{{\"o_id\":\"{}\",\"c_id\":\"{}\",\"r_id\":\"{}\",\"ordertime\":\"{}\",\"orderlines\":[],\"state\":\"{}\",\"cust_addr\":\"{}\",\"rest_addr\":\"{}\"}}", 
            order.o_id, order.c_id, order.r_id, order.ordertime, order.state, order.cust_addr, order.rest_addr);
        let mut mock_prod = MockKafkaProducer::new();
        mock_prod.expect_send()
            .withf(move |x, y| {
                x.eq("OrderCreated") && y.eq(&exp_json)
            })
            .times(1)
            .returning(|_x, _y| {
                Err(OrderServiceError::EventBrokerError(kafka::Error::CodecError))
            });
        let res = publish_order_created(order, &mut mock_prod);
        assert!(res.is_err());
    }
}