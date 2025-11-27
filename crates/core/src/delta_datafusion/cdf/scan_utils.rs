use std::collections::HashMap;
use std::path::Path as StdPath;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use chrono::TimeZone;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use object_store::path::Path;
use object_store::ObjectMeta;
use serde_json::Value;
use url::Url;

use crate::delta_datafusion::cdf::CHANGE_TYPE_COL;
use crate::delta_datafusion::cdf::{CdcDataSpec, FileAction};
use crate::delta_datafusion::{get_null_of_arrow_type, to_correct_scalar_value};
use crate::DeltaResult;

pub fn map_action_to_scalar<F: FileAction>(
    action: &F,
    part: &str,
    schema: SchemaRef,
) -> DeltaResult<ScalarValue> {
    Ok(action
        .partition_values()?
        .get(part)
        .map(|val| {
            schema
                .field_with_name(part)
                .map(|field| match val {
                    Some(value) => to_correct_scalar_value(
                        &Value::String(value.to_string()),
                        field.data_type(),
                    )
                    .unwrap_or(Some(ScalarValue::Null))
                    .unwrap_or(ScalarValue::Null),
                    None => get_null_of_arrow_type(field.data_type()).unwrap_or(ScalarValue::Null),
                })
                .unwrap_or(ScalarValue::Null)
        })
        .unwrap_or(ScalarValue::Null))
}

pub fn create_spec_partition_values<F: FileAction>(
    spec: &CdcDataSpec<F>,
    action_type: Option<&ScalarValue>,
) -> Vec<ScalarValue> {
    let mut spec_partition_values = action_type.cloned().map(|at| vec![at]).unwrap_or_default();
    spec_partition_values.push(ScalarValue::Int64(Some(spec.version)));
    spec_partition_values.push(ScalarValue::TimestampMillisecond(
        Some(spec.timestamp),
        None,
    ));
    spec_partition_values
}

pub fn create_partition_values<F: FileAction>(
    schema: SchemaRef,
    specs: Vec<CdcDataSpec<F>>,
    table_partition_cols: &[String],
    action_type: Option<ScalarValue>,
    // Base location (table root) used to normalize file paths for local file scheme
    base_location: &Url,
) -> DeltaResult<HashMap<Vec<ScalarValue>, Vec<PartitionedFile>>> {
    let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

    for spec in specs {
        let spec_partition_values = create_spec_partition_values(&spec, action_type.as_ref());

        for action in spec.actions {
            let partition_values = table_partition_cols
                .iter()
                .map(|part| map_action_to_scalar(&action, part, schema.clone()))
                .collect::<DeltaResult<Vec<ScalarValue>>>()?;

            let mut new_part_values = spec_partition_values.clone();
            new_part_values.extend(partition_values);

            // Normalize the action path similar to table scan behavior.
            // For file scheme:
            //  - keep absolute filesystem paths as-is
            //  - convert file:// URIs to filesystem paths
            //  - prefix table root directory for relative paths
            // For other schemes, keep the path as provided (relative to object store root).
            let normalized_path = {
                let p = action.path();
                if base_location.scheme() == "file" {
                    match Url::parse(p.as_str()) {
                        Ok(url) if url.scheme() == "file" => Path::parse(url.path())?,
                        Ok(_) => Path::parse(p.as_str())?,
                        Err(_) => {
                            if StdPath::new(p.as_str()).is_absolute() {
                                Path::parse(p.as_str())?
                            } else {
                                // Build absolute filesystem path under table root
                                let root_path = StdPath::new(base_location.path());
                                let full_path = root_path.join(p.as_str());
                                let full_str = full_path.to_str().ok_or_else(|| object_store::Error::Generic {
                                    store: "local",
                                    source: "Failed to convert path to string".into(),
                                })?;
                                Path::parse(full_str)?
                            }
                        }
                    }
                } else {
                    // Non-file scheme
                    Path::parse(p.as_str())?
                }
            };

            let part = PartitionedFile {
                object_meta: ObjectMeta {
                    location: normalized_path,
                    size: action.size()? as u64,
                    e_tag: None,
                    last_modified: chrono::Utc.timestamp_nanos(0),
                    version: None,
                },
                partition_values: new_part_values.clone(),
                extensions: None,
                range: None,
                statistics: None,
                metadata_size_hint: None,
            };

            file_groups.entry(new_part_values).or_default().push(part);
        }
    }
    Ok(file_groups)
}

pub fn create_cdc_schema(mut schema_fields: Vec<Arc<Field>>, include_type: bool) -> SchemaRef {
    if include_type {
        schema_fields.push(Field::new(CHANGE_TYPE_COL, DataType::Utf8, true).into());
    }
    Arc::new(Schema::new(schema_fields))
}
