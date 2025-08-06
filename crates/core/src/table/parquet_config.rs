use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::fmt;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ParquetConfig {
    pub table_parquet_options: HashMap<String, String>,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            table_parquet_options: HashMap::new(),
        }
    }
}

impl FromStr for ParquetConfig {
    type Err = String; // Or a custom error type

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let deserialized: ParquetConfig = serde_json::from_str(&s)
            .map_err(|e| format!("Failed to deserialize ParquetConfig: {}", e))?;;
        Ok(deserialized)
    }
}

// Implementing display also implements to_string
impl fmt::Display for ParquetConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let serialized = serde_json::to_string(&self).unwrap();
        write!(f, "{}", serialized)
    }
}