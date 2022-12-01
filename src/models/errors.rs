use std::fmt::Display;
#[derive(Debug)]
pub enum OrderServiceError {
    JSONParseError(serde_json::Error),
    TimeParseError(chrono::ParseError),
    IntParseError(std::num::ParseIntError),
    SplitColumnError(String),
    DBError(thrift::Error),
    RowNotFound(String),
    OrderBuildFailed(),
    EventBrokerError(kafka::Error)
}

impl Display for OrderServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderServiceError::JSONParseError(e) => write!(f, "JSONParseError: {}", e),
            OrderServiceError::DBError(e) => write!(f, "DBError: {}", e),
            OrderServiceError::TimeParseError(e) => write!(f, "TimeParseError: {}", e),
            OrderServiceError::IntParseError(e) => write!(f, "IntParseError: {}", e),
            OrderServiceError::EventBrokerError(e) => write!(f, "KafkaError: {}", e),
            OrderServiceError::RowNotFound(row) => write!(f, "Error: Row with id: '{}' was not found.", row),
            OrderServiceError::OrderBuildFailed() => write!(f, "Error building order from row content."),
            OrderServiceError::SplitColumnError(column) => write!(f, "Error splitting column - missing ':' character in string: {}", column),
        }
    }
}

impl From<serde_json::Error> for OrderServiceError {
    fn from(err: serde_json::Error) -> Self {
        OrderServiceError::JSONParseError(err)
    }
}

impl From<thrift::Error> for OrderServiceError {
    fn from(err: thrift::Error) -> Self {
        OrderServiceError::DBError(err)
    }
}

impl From<chrono::ParseError> for OrderServiceError {
    fn from(err: chrono::ParseError) -> Self {
        OrderServiceError::TimeParseError(err)
    }
}

impl From<std::num::ParseIntError> for OrderServiceError {
    fn from(err: std::num::ParseIntError) -> Self {
        OrderServiceError::IntParseError(err)
    }
}

impl From<kafka::Error> for OrderServiceError {
    fn from(err: kafka::Error) -> Self {
        OrderServiceError::EventBrokerError(err)
    }
}