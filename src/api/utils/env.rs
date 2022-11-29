use std::env;

pub fn get_env_var(var: &str) -> Option<String> {
    match env::var(var) {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

pub fn get_db_ip() -> Option<String> {
    get_env_var("HBASE_IP")
}

#[cfg(test)]
mod tests {

}