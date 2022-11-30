//TODO: Refactor for environment variables
const KAFKA_IP: &str = "10.245.211.251:9092";

fn raise_event(topic_name: &str) -> Result<(), ()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raise_event_is_ok() {
        let res = raise_event("topic");
        assert!(res.is_ok());
    }
}