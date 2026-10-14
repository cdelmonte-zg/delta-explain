use std::time::Duration;

use serde::de::DeserializeOwned;
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
    data_source_format: String,
}

struct UnityCatalogClient {
    http: reqwest::Client,
    config: CatalogConfig,
}

impl CatalogConfig {
    fn from_env() -> Result<Self> {
        let host = std::env::var("DATABRICKS_HOST").map_err(|_| {
            Error::Credentials("DATABRICKS_HOST is required with --catalog unity".to_string())
        })?;

        let token = std::env::var("DATABRICKS_TOKEN").map_err(|_| {
            Error::Credentials("DATABRICKS_TOKEN is required with --catalog unity".to_string())
        })?;

        Ok(Self { host, token })
    }
}

impl TryFrom<&str> for UnityTableName {
    type Error = Error;

    fn try_from(raw: &str) -> Result<Self> {
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

// A catalog that never answers must not hang the command: the kernel's
// own object-store reads have their timeouts, this covers the two REST
// calls made before any of that starts.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl UnityCatalogClient {
    fn new(config: CatalogConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|err| Error::Catalog(format!("Cannot build HTTP client: {err}")))?;

        Ok(Self { http, config })
    }

    fn endpoint(&self, path: &str) -> String {
        format!(
            "{}/api/2.1/unity-catalog/{path}",
            self.config.host.trim_end_matches('/')
        )
    }

    async fn get_table(&self, name: &UnityTableName) -> Result<TableInfo> {
        let full_name = format!("{}.{}.{}", name.catalog, name.schema, name.table);
        let request = self.http.get(self.endpoint(&format!("tables/{full_name}")));

        self.send(request).await
    }

    async fn get_temporary_credentials(&self, table_id: &str) -> Result<TemporaryCredentials> {
        let request = self
            .http
            .post(self.endpoint("temporary-table-credentials"))
            .json(&TemporaryCredentialsRequest {
                table_id,
                operation: "READ",
            });

        self.send::<TemporaryCredentialsResponse>(request)
            .await?
            .try_into()
    }

    // Every call shares one error contract: transport failures surface as
    // `CatalogRequest`; an error status keeps the server's own body, which
    // carries the `error_code` and `message` the user needs (a bucket the
    // server cannot vend for, a table it does not know).
    async fn send<T: DeserializeOwned>(&self, request: reqwest::RequestBuilder) -> Result<T> {
        let response = request.bearer_auth(&self.config.token).send().await?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(Error::Catalog(format!(
                "Unity Catalog request failed ({status}): {body}"
            )));
        }

        serde_json::from_str(&body)
            .map_err(|err| Error::Catalog(format!("invalid Unity Catalog response: {err}")))
    }
}

pub async fn resolve(name: &str) -> Result<ResolvedTable> {
    let config = CatalogConfig::from_env()?;
    let name = UnityTableName::try_from(name)?;
    let client = UnityCatalogClient::new(config)?;
    let table = client.get_table(&name).await?;

    if table.data_source_format != "DELTA" {
        return Err(Error::UnsupportedTable(format!(
            "table '{:?}' is not a Delta table",
            name
        )));
    }

    let location = table_uri::parse(&table.storage_location)?;

    let temporary = client.get_temporary_credentials(&table.table_id).await?;

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
        } = value;

        let credentials = match (
            aws_temp_credentials,
            azure_user_delegation_sas,
            gcp_oauth_token,
        ) {
            (Some(aws), None, None) => {
                let access_key_id = aws
                    .access_key_id
                    .ok_or_else(|| Error::Credentials("missing AWS access key id".to_string()))?;

                let secret_access_key = aws.secret_access_key.ok_or_else(|| {
                    Error::Credentials("missing AWS secret access key".to_string())
                })?;

                let session_token = aws
                    .session_token
                    .ok_or_else(|| Error::Credentials("missing AWS session token".to_string()))?;

                CatalogCredentials::Aws {
                    access_key_id,
                    secret_access_key,
                    session_token,
                }
            }

            (None, Some(azure), None) => {
                let sas_token = azure
                    .sas_token
                    .ok_or_else(|| Error::Credentials("missing Azure SAS token".to_string()))?;

                CatalogCredentials::Azure { sas_token }
            }

            (None, None, Some(gcp)) => {
                let oauth_token = gcp
                    .oauth_token
                    .ok_or_else(|| Error::Credentials("missing GCP OAuth token".to_string()))?;

                CatalogCredentials::Gcp { oauth_token }
            }

            _ => {
                return Err(Error::Credentials(
                    "Unity Catalog returned invalid temporary credentials".to_string(),
                ));
            }
        };

        Ok(Self { credentials })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials(json: &str) -> Result<CatalogCredentials> {
        let response: TemporaryCredentialsResponse = serde_json::from_str(json).unwrap();
        TemporaryCredentials::try_from(response).map(|temporary| temporary.credentials)
    }

    #[test]
    fn table_name_has_three_non_empty_parts() {
        let name = UnityTableName::try_from("lake.default.users").unwrap();
        assert_eq!(
            (
                name.catalog.as_str(),
                name.schema.as_str(),
                name.table.as_str()
            ),
            ("lake", "default", "users")
        );

        for raw in [
            "users",
            "lake.users",
            "lake.default.users.extra",
            "lake..users",
            "",
        ] {
            let err = UnityTableName::try_from(raw).unwrap_err();
            assert!(matches!(err, Error::TableUri(_)), "{raw}: {err}");
        }
    }

    #[test]
    fn aws_credentials_are_mapped_and_optional_fields_ignored() {
        // The shape Unity Catalog OSS returns for statically configured
        // credentials: no expiration, the table's own url.
        let json = r#"{
            "aws_temp_credentials": {
                "access_key_id": "AKIA", "secret_access_key": "secret",
                "session_token": "token"
            },
            "azure_user_delegation_sas": null, "gcp_oauth_token": null,
            "expiration_time": null, "url": "s3://cat/uc-external/users"
        }"#;

        match credentials(json).unwrap() {
            CatalogCredentials::Aws {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                assert_eq!(access_key_id, "AKIA");
                assert_eq!(secret_access_key, "secret");
                assert_eq!(session_token, "token");
            }
            other => panic!("expected AWS credentials, got {other:?}"),
        }
    }

    #[test]
    fn azure_and_gcp_credentials_are_mapped() {
        let azure = credentials(r#"{"azure_user_delegation_sas": {"sas_token": "sv=2024"}}"#);
        assert!(
            matches!(azure.unwrap(), CatalogCredentials::Azure { sas_token } if sas_token == "sv=2024")
        );

        let gcp = credentials(r#"{"gcp_oauth_token": {"oauth_token": "ya29"}}"#);
        assert!(
            matches!(gcp.unwrap(), CatalogCredentials::Gcp { oauth_token } if oauth_token == "ya29")
        );
    }

    #[test]
    fn missing_or_ambiguous_credentials_are_rejected() {
        let cases = [
            r#"{}"#,
            r#"{"aws_temp_credentials": {"access_key_id": "AKIA", "secret_access_key": "s"}}"#,
            r#"{"azure_user_delegation_sas": {}}"#,
            r#"{"aws_temp_credentials": {"access_key_id": "a", "secret_access_key": "s", "session_token": "t"},
                "azure_user_delegation_sas": {"sas_token": "x"}}"#,
        ];

        for json in cases {
            let err = credentials(json).unwrap_err();
            assert!(matches!(err, Error::Credentials(_)), "{json}: {err}");
        }
    }
}
