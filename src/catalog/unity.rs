use serde::{Deserialize, Serialize};

use crate::catalog::{CatalogCredentials, ResolvedStorage, ResolvedTable};
use crate::error::{Error, Result};
use crate::table_uri;

struct CatalogConfig {
    host: String,
    token: String,
}

#[derive(Clone, Debug)]
struct UnityTableName {
    catalog: String,
    schema: String,
    table: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct TableInfo {
    table_id: String,
    storage_location: String,
    table_type: String,
    data_source_format: String,
}

struct UnityCatalogClient {
    http: reqwest::Client,
    config: CatalogConfig,
}

impl CatalogConfig {
    fn from_env() -> Result<Self> {
        let host = std::env::var("DATABRICKS_HOST").map_err(|_| {
            Error::Credentials(
                "DATABRICKS_HOST is required with --catalog unity".to_string()
            )
        })?;

        let token = std::env::var("DATABRICKS_TOKEN").map_err(|_| {
            Error::Credentials(
                "DATABRICKS_TOKEN is required with --catalog unity".to_string()
            )
        })?;

        Ok(Self { host, token })
    }
}

impl TryFrom<&str> for UnityTableName {
    type Error = Error;

    fn try_from(raw: &str) -> Result<Self>{
        let parts = raw.split('.').collect::<Vec<_>>();

        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return Err(Error::TableUri(format!(
                "Invalid Unity Catalog table name '{raw}', expected catalog.schema.table"
            )));
        }
        
        Ok(Self {
            catalog: parts[0].to_string(),
            schema: parts[1].to_string(),
            table: parts[2].to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct TemporaryCredentials {
    credentials: CatalogCredentials,
    expiration_time: Option<i64>,
    url: Option<String>,
}


#[derive(Debug, Serialize)]
struct TemporaryCredentialsRequest<'a> {
    table_id: &'a str,
    operation: &'static str,
}

#[derive(Debug, Deserialize)]
struct TemporaryCredentialsResponse {
    aws_temp_credentials: Option<AwsCredentialResponse>,
    azure_user_delegation_sas: Option<AzureCredentialResponse>,
    gcp_oauth_token: Option<GcpCredentialResponse>,
    expiration_time: Option<i64>,
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AwsCredentialResponse {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AzureCredentialResponse {
    sas_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GcpCredentialResponse {
    oauth_token: Option<String>,
}

impl UnityCatalogClient {
    fn new(config: CatalogConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config
        }
    }
    
    async fn get_table(&self, name: &UnityTableName) -> Result<TableInfo> {
        let full_name = format!("{}.{}.{}", name.catalog,name.schema, name.table);

        let url = format!("{}/api/2.1/unity-catalog/tables/{}", 
            self.config.host.trim_end_matches('/'), 
            full_name
        );

        let response = self
            .http
            .get(url)
            .bearer_auth(&self.config.token)
            .send()
            .await?
            .error_for_status()?;

        let table = response.json::<TableInfo>().await?;

        Ok(table)
    }

    async fn get_temporary_credentials(&self, table_id: &str) -> Result<TemporaryCredentials> {
        let url = format!("{}/api/2.1/unity-catalog/temporary-table-credentials",
            self.config.host.trim_end_matches('/')
        );

        let request = TemporaryCredentialsRequest {
            table_id,
            operation: "READ",
        };

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.config.token)
            .json(&request)
            .send()
            .await?;
            

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(Error::Catalog(format!(
                "Unity Catalog request failed ({status}): {body}"
            )));
        }

        let response =
            serde_json::from_str::<TemporaryCredentialsResponse>(&body)?;

        response.try_into()
    } 
}

pub async fn resolve(name : &str) -> Result<ResolvedTable> {
    let config = CatalogConfig::from_env()?;
    let name = UnityTableName::try_from(name)?;
    let client = UnityCatalogClient::new(config);
    let table = client.get_table(&name).await?;

    if table.data_source_format != "DELTA" {
        return Err(Error::UnsupportedTable(format!(
            "table '{:?}' is not a Delta table",
            name
        )));
    }
    dbg!("{}", &table);

    let location = table_uri::parse(&table.storage_location)?;

    let temporary = client
    .get_temporary_credentials(&table.table_id)
    .await?;

    Ok(ResolvedTable {
        location,
        storage: ResolvedStorage::CatalogVended {
            credentials: temporary.credentials,
        },
    })
}

impl TryFrom<TemporaryCredentialsResponse> for TemporaryCredentials {
    type Error = Error;

    fn try_from(value: TemporaryCredentialsResponse) -> Result<Self> {
        let TemporaryCredentialsResponse {
            aws_temp_credentials,
            azure_user_delegation_sas,
            gcp_oauth_token,
            expiration_time,
            url,
        } = value;

        let credentials = match (
            aws_temp_credentials,
            azure_user_delegation_sas,
            gcp_oauth_token,
        ) {
            (Some(aws), None, None) => {
                let access_key_id = aws.access_key_id.ok_or_else(
                    || Error::Credentials("missing AWS access key id".to_string()))?;
                
                let secret_access_key = aws.secret_access_key.ok_or_else(
                    || Error::Credentials("missing AWS secret access key".to_string()))?;

                let session_token = aws.session_token.ok_or_else(|| {
                    Error::Credentials(
                        "missing AWS session token".to_string()
                    )
                })?;

                CatalogCredentials::Aws {
                        access_key_id,
                        secret_access_key,
                        session_token,
                }
            }
            
            (None, Some(azure), None) => {
                let sas_token = azure.sas_token.ok_or_else(
                    || Error::Credentials("missing Azure SAS token".to_string()))?;
                
                CatalogCredentials::Azure { sas_token}
            },

            (None, None, Some(gcp)) => {
                let oauth_token = gcp.oauth_token.ok_or_else(
                    || Error::Credentials(("missing GCP OAuth token".to_string())))?;

                CatalogCredentials::Gcp { oauth_token }
            }

            _ => {
                return Err(Error::Credentials(
                "Unity Catalog returned invalid temporary credentials".to_string()))
            }
        };

        Ok(Self {
            credentials,
            expiration_time,
            url
        })
    }
}