use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{DynObjectStore, ObjectStore, ObjectStoreExt};
use serde_json::Value;
use url::Url;

use crate::v2::error::Error;

#[derive(Debug)]
pub struct LogMetadata {
    pub partition_columns: Vec<String>,
    pub clustering_domain: Option<String>,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
}

pub fn read_log_metadata(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
    max_version: Option<u64>,
) -> Result<LogMetadata, Error> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Storage(format!("Cannot create tokio runtime: {e}")))?;

    rt.block_on(read_log_metadata_async(table_url, store, max_version))
}

fn log_prefix_for(table_url: &Url) -> Result<ObjectPath, Error> {
    let table_prefix = ObjectPath::from_url_path(table_url.path().trim_matches('/'))
        .map_err(|e| Error::Storage(format!("Cannot derive log path from table URL: {e}")))?;

    Ok(if table_prefix.as_ref().is_empty() {
        ObjectPath::from("_delta_log")
    } else {
        ObjectPath::from(format!("{}/_delta_log", table_prefix.as_ref()))
    })
}

fn commit_version(path: &ObjectPath) -> Option<u64> {
    let filename = path.as_ref().rsplit('/').next()?;

    let version = filename.strip_suffix(".json")?;

    if version.len() != 20 || !version.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    version.parse().ok()
}

async fn read_log_metadata_async(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
    max_version: Option<u64>,
) -> Result<LogMetadata, Error> {
    let log_prefix = log_prefix_for(table_url)?;

    let objects: Vec<_> = store
        .list(Some(&log_prefix))
        .try_collect()
        .await
        .map_err(|e| Error::Storage(format!("Cannot list delta log: {e}")))?;

    let mut json_paths = objects
        .into_iter()
        .filter_map(|obj| {
            let version = commit_version(&obj.location)?;

            let within_range = match max_version {
                Some(max) => version <= max,

                None => true,
            };

            within_range.then_some(obj.location)
        })
        .collect::<Vec<_>>();

    json_paths.sort();

    let mut partition_columns = Vec::new();

    let mut clustering_domain = None;

    let mut reader_features = Vec::new();

    let mut writer_features = Vec::new();

    for path in json_paths {
        let data = store
            .get(&path)
            .await
            .map_err(|e| Error::Storage(format!("Cannot read {path}: {e}")))?
            .bytes()
            .await
            .map_err(|e| Error::Storage(format!("Cannot read bytes {path}: {e}")))?;

        let content = String::from_utf8_lossy(&data);

        for line in content.lines() {
            let Ok(action) = serde_json::from_str::<Value>(line) else {
                continue;
            };

            if let Some(meta) = action.get("metaData")
                && let Some(cols) = meta.get("partitionColumns").and_then(Value::as_array)
            {
                partition_columns = cols
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            }

            if let Some(protocol) = action.get("protocol") {
                let feature_list = |key: &str| -> Vec<String> {
                    protocol
                        .get(key)
                        .and_then(Value::as_array)
                        .map(|features| {
                            features
                                .iter()
                                .filter_map(|f| f.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default()
                };

                reader_features = feature_list("readerFeatures");

                writer_features = feature_list("writerFeatures");
            }

            if let Some(domain) = action.get("domainMetadata")
                && domain.get("domain").and_then(Value::as_str) == Some("delta.clustering")
            {
                let removed = domain
                    .get("removed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                clustering_domain = if removed {
                    None
                } else {
                    domain
                        .get("configuration")
                        .and_then(Value::as_str)
                        .map(String::from)
                        .or(Some(String::new()))
                };
            }
        }
    }

    Ok(LogMetadata {
        partition_columns,
        clustering_domain,
        reader_features,
        writer_features,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_prefix_is_derived_from_table_path() {
        let cases = [
            ("s3://bucket/prefix/table", "prefix/table/_delta_log"),
            ("s3://bucket/table", "table/_delta_log"),
            ("s3://bucket", "_delta_log"),
            ("az://container/table", "table/_delta_log"),
            ("gs://bucket/lake/users", "lake/users/_delta_log"),
            ("file:///home/user/table", "home/user/table/_delta_log"),
        ];

        for (url, expected) in cases {
            let url = Url::parse(url).unwrap();

            assert_eq!(
                log_prefix_for(&url).unwrap().as_ref(),
                expected,
                "url: {url}"
            );
        }
    }

    #[test]
    fn commit_version_is_parsed_from_delta_json_path() {
        let path = ObjectPath::from("_delta_log/00000000000000000042.json");

        assert_eq!(commit_version(&path), Some(42));
    }

    #[test]
    fn commit_version_rejects_non_commit_json() {
        let cases = [
            "_delta_log/foo.json",
            "_delta_log/42.json",
            "_delta_log/00000000000000000042.crc",
            "_delta_log/_last_checkpoint",
        ];

        for path in cases {
            assert_eq!(
                commit_version(&ObjectPath::from(path)),
                None,
                "path: {path}"
            );
        }
    }
}
