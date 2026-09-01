mod aws_profile;
mod environment;
mod options;

use std::str::FromStr;
use std::sync::Arc;

use delta_kernel::Engine;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;
use object_store::DynObjectStore;
use url::Url;

use crate::v2::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageOption {
    pub key: String,
    pub value: String,
}

impl FromStr for StorageOption {
    type Err = Error;

    fn from_str(raw: &str) -> Result<Self> {
        let (key, value) = raw.split_once('=').ok_or_else(|| {
            Error::Options(format!("Invalid option format '{raw}', expected KEY=VALUE"))
        })?;

        Ok(Self {
            key: key.to_ascii_lowercase(),

            value: value.to_string(),
        })
    }
}

impl StorageOption {
    pub(super) fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    pub env_credentials: bool,

    pub profile: Option<String>,

    pub region: Option<String>,

    pub public: bool,

    pub options: Vec<StorageOption>,
}

pub struct StorageRuntime {
    pub store: Arc<DynObjectStore>,

    pub engine: Box<dyn Engine>,
}

pub fn open(url: &Url, config: &StorageConfig) -> Result<StorageRuntime> {
    let resolved = options::resolve(config)?;

    let store = store_from_url_opts(
        url,
        resolved
            .into_iter()
            .map(|option| (option.key, option.value)),
    )?;

    let engine = DefaultEngineBuilder::new(store.clone()).build();

    Ok(StorageRuntime {
        store,

        engine: Box::new(engine),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_custom_option() {
        let option = "REGION=eu-central-1".parse::<StorageOption>().unwrap();

        assert_eq!(
            option,
            StorageOption {
                key: "region".to_string(),

                value: "eu-central-1".to_string(),
            }
        );
    }

    #[test]
    fn rejects_custom_option_without_equals() {
        let err = "region".parse::<StorageOption>().unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid option format 'region', expected KEY=VALUE"
        );
    }
}
