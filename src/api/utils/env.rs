use std::env;

pub const DB_IP_ENV_ERR_MSG: &str = "Error finding database ip environment variable. Contact system administrator";
pub const HBASE_DB_ENV_VAR: &str = "HBASE_IP";

pub const KAFKA_IP_ENV_ERR_MSG: &str = "Error finding event-broker ip environment variable. Contact system administrator";
pub const KAFKA_ENV_VAR: &str = "KAFKA_IP";

pub fn get_env_var(var: &str) -> Option<String> {
    match env::var(var) {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

pub fn get_db_ip() -> Option<String> {
    get_env_var(HBASE_DB_ENV_VAR)
}

pub fn get_kafka_ip() -> Option<String> {
    get_env_var(KAFKA_ENV_VAR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::{set_var, remove_var};
    
    #[test]
    #[ignore = "These tests interact in a way that make them fail randomly."]
    fn test_get_env_var_not_set() {
        let res = get_env_var("VAR_THAT_DOES_NOT_EXIST");
        assert!(res.is_none());
    }

    #[test]
    #[ignore = "These tests interact in a way that make them fail randomly."]
    fn test_get_env_var() {
        let var_name = "IS_SET";
        let exp_var_value = "this is from a env var";
        set_var(var_name, exp_var_value);
        let res = get_env_var(var_name);
        assert!(res.is_some());
        let act_value = res.unwrap();
        assert_eq!(act_value, exp_var_value);
    }

    #[test]
    #[ignore = "These tests interact in a way that make them fail randomly."]
    fn test_get_db_ip_not_set() {
        remove_var(HBASE_DB_ENV_VAR);
        let res = get_db_ip();
        assert!(res.is_none());
    }

    #[test]
    #[ignore = "These tests interact in a way that make them fail randomly."]
    fn test_get_db_ip() {
        let exp_var_value = "123.45.67.89:1011";
        set_var(HBASE_DB_ENV_VAR, exp_var_value);
        let res = get_env_var(HBASE_DB_ENV_VAR);
        assert!(res.is_some());
        let act_value = res.unwrap();
        assert_eq!(act_value, exp_var_value);
    }

    #[test]
    #[ignore = "These tests interact in a way that make them fail randomly."]
    fn test_get_kafka_ip_not_set() {
        remove_var(KAFKA_ENV_VAR);
        let res = get_db_ip();
        assert!(res.is_none());
    }

    #[test]
    #[ignore = "These tests interact in a way that make them fail randomly."]
    fn test_get_kafka_ip() {
        let exp_var_value = "123.45.67.89:1011";
        set_var(KAFKA_ENV_VAR, exp_var_value);
        let res = get_env_var(KAFKA_ENV_VAR);
        assert!(res.is_some());
        let act_value = res.unwrap();
        assert_eq!(act_value, exp_var_value);
    }
}