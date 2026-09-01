use super::StorageOption;

const ENV_CREDENTIAL_MAP: &[(&str, &str)] = &[
    ("AWS_DEFAULT_REGION", "region"),
    ("AWS_REGION", "region"),
    ("AWS_ACCESS_KEY_ID", "access_key_id"),
    ("AWS_SECRET_ACCESS_KEY", "secret_access_key"),
    ("AWS_SESSION_TOKEN", "session_token"),
    ("AWS_ENDPOINT_URL", "endpoint"),
    ("AZURE_STORAGE_ACCOUNT_NAME", "account_name"),
    ("AZURE_STORAGE_ACCOUNT_KEY", "account_key"),
    ("GOOGLE_SERVICE_ACCOUNT", "google_service_account"),
    (
        "GOOGLE_APPLICATION_CREDENTIALS",
        "google_application_credentials",
    ),
];

pub(super) fn resolve() -> Vec<StorageOption> {
    resolve_with(|name| std::env::var(name).ok())
}

fn resolve_with(get: impl Fn(&str) -> Option<String>) -> Vec<StorageOption> {
    ENV_CREDENTIAL_MAP
        .iter()
        .filter_map(|(var, key)| get(var).map(|value| StorageOption::new(*key, value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_environment_variables() {
        let fake = |var: &str| match var {
            "AWS_ACCESS_KEY_ID" => Some("AKIA123".to_string()),

            "AWS_SECRET_ACCESS_KEY" => Some("secret".to_string()),

            "AWS_REGION" => Some("eu-central-1".to_string()),

            "GOOGLE_APPLICATION_CREDENTIALS" => Some("/path/key.json".to_string()),

            _ => None,
        };

        let options = resolve_with(fake);

        assert!(options.contains(&StorageOption::new("access_key_id", "AKIA123",)));

        assert!(options.contains(&StorageOption::new("secret_access_key", "secret",)));

        assert!(options.contains(&StorageOption::new("region", "eu-central-1",)));

        assert!(options.contains(&StorageOption::new(
            "google_application_credentials",
            "/path/key.json",
        )));
    }

    #[test]
    fn aws_region_follows_default_region() {
        let fake = |var: &str| match var {
            "AWS_DEFAULT_REGION" => Some("us-east-1".to_string()),

            "AWS_REGION" => Some("eu-central-1".to_string()),

            _ => None,
        };

        let options = resolve_with(fake);

        let regions = options
            .iter()
            .filter(|option| option.key == "region")
            .collect::<Vec<_>>();

        assert_eq!(
            regions.last().map(|option| { option.value.as_str() },),
            Some("eu-central-1")
        );
    }

    #[test]
    fn empty_environment_yields_no_options() {
        assert!(resolve_with(|_| None).is_empty());
    }
}
