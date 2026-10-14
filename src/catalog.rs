mod unity;

use crate::error::{Error, Result};
use crate::table_uri;

pub enum TableReference {
    Path(String),
    UnityCatalog { name: String },
}

pub struct ResolvedTable {
    pub location: url::Url,
    pub storage: ResolvedStorage,
}

pub enum ResolvedStorage {
    UserConfigured,
    CatalogVended { credentials: CatalogCredentials },
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

pub fn resolve(reference: TableReference) -> Result<ResolvedTable> {
    match reference {
        TableReference::Path(path) => {
            let location = table_uri::parse(&path)?;

            Ok(ResolvedTable {
                location,
                storage: ResolvedStorage::UserConfigured,
            })
        }
        TableReference::UnityCatalog { name } => {
            // Two serial HTTP calls: a current-thread runtime is enough,
            // and it does not compete with the pool the kernel engine owns.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| Error::Catalog(format!("Cannot create catalog runtime: {err}")))?;

            runtime.block_on(unity::resolve(&name))
        }
    }
}
