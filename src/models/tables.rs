use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize)]
pub struct TableName {
    pub table_name: String,
}

impl TableName {
    pub fn new(name: String ) -> Self {
        Self{table_name: name}
    }
}

impl Into<String> for TableName {
    fn into(self) -> String {
        self.table_name
    }
}
