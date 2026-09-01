use std::collections::HashMap;
use std::path::PathBuf;

use crate::error::{Error, Result};

use super::StorageOption;

pub(super) fn resolve(profile: &str) -> Result<Vec<StorageOption>> {
    let credentials = read_aws_file("AWS_SHARED_CREDENTIALS_FILE", ".aws/credentials", false)?;

    let config = read_aws_file("AWS_CONFIG_FILE", ".aws/config", true)?;

    let cred_section = credentials.get(profile);

    let conf_section = config.get(profile);

    if cred_section.is_none() && conf_section.is_none() {
        return Err(Error::Credentials(format!(
            "AWS profile '{profile}' not found in ~/.aws/credentials or ~/.aws/config"
        )));
    }

    // Like the AWS CLI, credentials-file
    // values win over config-file values.
    let lookup = |key: &str| {
        cred_section
            .and_then(|section| section.get(key))
            .or_else(|| conf_section.and_then(|section| section.get(key)))
            .cloned()
    };

    let mut options = Vec::new();

    match (lookup("aws_access_key_id"), lookup("aws_secret_access_key")) {
        (Some(id), Some(secret)) => {
            options.push(StorageOption::new("access_key_id", id));

            options.push(StorageOption::new("secret_access_key", secret));
        }

        _ => {
            let mechanism = [
                "sso_start_url",
                "sso_session",
                "credential_process",
                "role_arn",
            ]
            .iter()
            .find(|key| lookup(key).is_some());

            return Err(match mechanism {
                Some(mechanism) => Error::Credentials(format!(
                    "AWS profile '{profile}' uses {mechanism}, which delta-explain does not resolve. \
                                 Export static credentials first: \
                                 eval $(aws configure export-credentials --profile {profile} --format env) \
                                 and use --env-creds"
                )),

                None => Error::Credentials(format!(
                    "AWS profile '{profile}' has no aws_access_key_id / aws_secret_access_key"
                )),
            });
        }
    }

    if let Some(token) = lookup("aws_session_token") {
        options.push(StorageOption::new("session_token", token));
    }

    if let Some(region) = lookup("region") {
        options.push(StorageOption::new("region", region));
    }

    Ok(options)
}

fn read_aws_file(
    env_override: &str,
    home_relative: &str,
    config_style: bool,
) -> Result<HashMap<String, HashMap<String, String>>> {
    let path = match std::env::var_os(env_override) {
        Some(path) => PathBuf::from(path),

        None => match std::env::home_dir() {
            Some(home) => home.join(home_relative),

            None => {
                return Ok(HashMap::new());
            }
        },
    };

    let Ok(content) = std::fs::read_to_string(&path) else {
        return Ok(HashMap::new());
    };

    Ok(parse_ini(&content, config_style))
}

fn parse_ini(content: &str, config_style: bool) -> HashMap<String, HashMap<String, String>> {
    let mut sections = HashMap::new();

    let mut current: Option<String> = None;

    for raw in content.lines() {
        let line = raw.trim();

        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        if let Some(header) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            let name = header.trim();

            let name = if config_style {
                name.strip_prefix("profile ").unwrap_or(name).trim()
            } else {
                name
            };

            current = Some(name.to_string());

            sections
                .entry(name.to_string())
                .or_insert_with(HashMap::new);

            continue;
        }

        if let (Some(section), Some((key, value))) = (&current, line.split_once('=')) {
            let key = key.trim().to_ascii_lowercase();

            let value = value.trim();

            if !value.is_empty()
                && let Some(map) = sections.get_mut(section)
            {
                map.insert(key, value.to_string());
            }
        }
    }

    sections
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_credentials_style_sections() {
        let ini = "
[default]
aws_access_key_id = AKIADEFAULT
aws_secret_access_key = s3cret

[dev]
aws_access_key_id = AKIADEV
aws_secret_access_key = devsecret
aws_session_token = tok
";

        let parsed = parse_ini(ini, false);

        assert_eq!(parsed["default"]["aws_access_key_id"], "AKIADEFAULT");

        assert_eq!(parsed["dev"]["aws_session_token"], "tok");
    }

    #[test]
    fn config_style_strips_profile_prefix() {
        let ini = "
[profile dev]
region = eu-central-1

[default]
region = us-east-1
";

        let parsed = parse_ini(ini, true);

        assert_eq!(parsed["dev"]["region"], "eu-central-1");

        assert_eq!(parsed["default"]["region"], "us-east-1");
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let ini = "
# a comment
[p]
; another
aws_access_key_id = X
";

        let parsed = parse_ini(ini, false);

        assert_eq!(parsed["p"]["aws_access_key_id"], "X");
    }
}
