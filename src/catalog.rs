mod unity;

use serde::Deserialize;

use crate::error::Result;
use crate::table_uri;


pub enum TableReference {
    Path(String),
    UnityCatalog {
        name: String,
    }
}

pub struct ResolvedTable {
    pub location: url::Url,
    pub storage: ResolvedStorage
}

pub enum ResolvedStorage {
    UserConfigured,
    CatalogVended { 
        credentials: CatalogCredentials,
    },
}

#[derive(Clone, Debug)]
pub enum CatalogCredentials {
    Aws {
        access_key_id: String,
        secret_access_key: String,
        session_token: String,
    },
    Azure {
        sas_token: String,
    },
    Gcp {
        oauth_token: String,
    },
}

pub async fn resolve(reference: TableReference) -> Result<ResolvedTable> {

    match reference {
        TableReference::Path(path) => {
            let location = table_uri::parse(&path)?;

            Ok(
                ResolvedTable { 
                location, 
                storage: ResolvedStorage::UserConfigured, 
                }
            )
        }
        TableReference::UnityCatalog {name} => {
            unity::resolve(&name).await
        }
    }
}
