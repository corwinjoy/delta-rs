// Utilities to represent TableParquetOptions as a set of key-value pairs
// Only active when the "datafusion" feature is enabled

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::fmt;
use std::fmt::Display;
use datafusion::common::{DataFusionError, file_options::parquet_writer::ParquetWriterOptions};
use datafusion::config::{ConfigEntry, ConfigField, TableParquetOptions, Visit};
use parquet::file::properties::WriterProperties;

// String representation of the Parquet configuration.
// This hashmap holds key-value pairs for the settings in
// datafusion_common::config::TableParquetOptions
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
    type Err = String; // Should hold a project standard error type?

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


// Function to convert TableParquetOptions to a vector of ConfigEntry
// This should probably be part of DataFusion's config module but is placed here for now.
pub fn entries(table_parquet_options: &TableParquetOptions) -> Vec<ConfigEntry> {
    struct Visitor(Vec<ConfigEntry>);

    impl Visit for Visitor {
        fn some<V: Display>(
            &mut self,
            key: &str,
            value: V,
            description: &'static str,
        ) {
            self.0.push(ConfigEntry {
                key: key.to_string(),
                value: Some(value.to_string()),
                description,
            })
        }

        fn none(&mut self, key: &str, description: &'static str) {
            self.0.push(ConfigEntry {
                key: key.to_string(),
                value: None,
                description,
            })
        }
    }

    let mut v = Visitor(vec![]);
    table_parquet_options.visit(&mut v, "", "");
    v.0
}

impl From<TableParquetOptions> for ParquetConfig {
    fn from(table_parquet_options: TableParquetOptions) -> Self {
        let dpo: HashMap<String, String> = Self::map_table_entries(&TableParquetOptions::default());
        let tpo: HashMap<String, String> = Self::map_table_entries(&table_parquet_options);
        
        println!("[parquet_config] Default options: {:#?}", dpo);
        println!("[parquet_config] Table options: {:#?}", tpo);
        let mut altered: HashMap<String, String> = HashMap::new();

        // Find altered entries
        for (k, v_tpo) in tpo {
            match dpo.get(&k) {
                Some(v_dpo) => {
                    if *v_dpo != v_tpo {
                        altered.insert(k.clone(), v_tpo.clone());
                    }
                }
                None => {altered.insert(k.clone(), v_tpo.clone()); }
            }
        }
        ParquetConfig{
            table_parquet_options: altered,
        }
    }
}

impl TryInto<TableParquetOptions> for ParquetConfig {
    type Error = DataFusionError;
    fn try_into(self) -> Result<TableParquetOptions, Self::Error> {
        let mut options = TableParquetOptions::default();
        for (key, value) in self.table_parquet_options {
            if key.len() < 1 || value.len() < 1 {
                continue;
            }
            // println!("Setting key: {}, value: {}", key, value);
            options.set(&*key, &*value)?;
        }
        Ok(options)
    }
}


impl ParquetConfig {
    /// Create a new ParquetConfig with default options.
    pub fn new() -> Self {
        ParquetConfig::default()
    }

    // Convert the ParquetConfig to parquet::file::properties::WriterProperties
    #[cfg(feature = "datafusion")]
    pub fn writer_options(&self) -> WriterProperties {

        let mut table_parquet_options: TableParquetOptions = self.clone().try_into()
            .expect("Failed to convert ParquetConfig to TableParquetOptions");
        
        table_parquet_options.global.skip_arrow_metadata = true;

        // Convert TableParquetOptions to ParquetWriterOptions
        let writer_options : ParquetWriterOptions =
            ParquetWriterOptions::try_from(&table_parquet_options)
                .expect("Failed to convert TableParquetOptions to ParquetWriterOptions");

        writer_options.writer_options().clone()
    }

    fn map_table_entries(table_parquet_options: &TableParquetOptions) -> HashMap<String, String> {
        entries(&table_parquet_options)
            .into_iter()
            .map(|entry| {
                let key_as_str = entry.key.as_str();
                let stripped_key = &key_as_str[1..];
                (String::from(stripped_key), entry.value.unwrap_or_default())
            })
            .collect::<HashMap<String, String>>()
            .into()
    }
}