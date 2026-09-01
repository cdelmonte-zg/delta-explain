use crate::error::Result;

use super::{StorageConfig, StorageOption, aws_profile, environment};

pub(super) fn resolve(config: &StorageConfig) -> Result<Vec<StorageOption>> {
    resolve_with(config, environment::resolve, aws_profile::resolve)
}

fn resolve_with(
    config: &StorageConfig,

    environment_resolver: impl Fn() -> Vec<StorageOption>,

    profile_resolver: impl Fn(&str) -> Result<Vec<StorageOption>>,
) -> Result<Vec<StorageOption>> {
    let mut resolved = Vec::new();

    // Least explicit.
    if config.env_credentials {
        resolved.extend(environment_resolver());
    }

    if let Some(profile) = &config.profile {
        resolved.extend(profile_resolver(profile)?);
    }

    if let Some(region) = &config.region {
        resolved.push(StorageOption::new("region", region.clone()));
    }

    if config.public {
        resolved.push(StorageOption::new("skip_signature", "true"));
    }

    // Most explicit.
    //
    // Deliberately append rather than merge into a HashMap:
    // store_from_url_opts processes the iterator in order,
    // and object_store accepts aliases for several backend
    // options. Appending guarantees that --option remains
    // the highest-precedence layer even when it uses a
    // different alias for the same backend setting.
    resolved.extend(config.options.iter().cloned());

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layers_are_resolved_in_precedence_order() {
        let config = StorageConfig {
            env_credentials: true,

            profile: Some("dev".to_string()),

            region: Some("eu-west-1".to_string()),

            public: true,

            options: vec![
                StorageOption::new("region", "ap-south-1"),
                StorageOption::new("skip_signature", "false"),
            ],
        };

        let result = resolve_with(
            &config,
            || vec![StorageOption::new("region", "us-east-1")],
            |_| Ok(vec![StorageOption::new("region", "eu-central-1")]),
        )
        .unwrap();

        assert_eq!(
            result,
            vec![
                StorageOption::new("region", "us-east-1",),
                StorageOption::new("region", "eu-central-1",),
                StorageOption::new("region", "eu-west-1",),
                StorageOption::new("skip_signature", "true",),
                StorageOption::new("region", "ap-south-1",),
                StorageOption::new("skip_signature", "false",),
            ]
        );
    }

    #[test]
    fn disabled_layers_are_not_resolved() {
        let config = StorageConfig::default();

        let result = resolve_with(
            &config,
            || {
                panic!("environment resolver should not run");
            },
            |_| {
                panic!("profile resolver should not run");
            },
        )
        .unwrap();

        assert!(result.is_empty());
    }
}
