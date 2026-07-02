//! AWS shared-config credential resolution for `--profile`.
//!
//! Resolves static credentials from the standard AWS shared files, the same
//! ones the AWS CLI reads: `~/.aws/credentials` (overridable via
//! `AWS_SHARED_CREDENTIALS_FILE`) and `~/.aws/config` (via
//! `AWS_CONFIG_FILE`). This covers the everyday laptop case that raw
//! environment variables (`--env-creds`) do not.
//!
//! Deliberately NOT a full SDK credential chain: profiles that rely on SSO,
//! `credential_process`, or role assumption produce a clear error pointing
//! at `aws configure export-credentials`, instead of pulling the whole AWS
//! SDK into the dependency tree for a diagnostic CLI.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::error::Error;

/// Resolve the named profile into object-store options
/// (`aws_access_key_id`, `aws_secret_access_key`, optionally
/// `aws_session_token` and `region`).
pub fn resolve_aws_profile(profile: &str) -> Result<HashMap<String, String>, Error> {
    let credentials = read_aws_file(
        "AWS_SHARED_CREDENTIALS_FILE",
        ".aws/credentials",
        /* config_style */ false,
    )?;
    let config = read_aws_file(
        "AWS_CONFIG_FILE",
        ".aws/config",
        /* config_style */ true,
    )?;

    let cred_section = credentials.get(profile);
    let conf_section = config.get(profile);
    if cred_section.is_none() && conf_section.is_none() {
        return Err(Error::Credentials(format!(
            "AWS profile '{profile}' not found in ~/.aws/credentials or ~/.aws/config"
        )));
    }

    // The credentials file wins over the config file, like the AWS CLI.
    let lookup = |key: &str| {
        cred_section
            .and_then(|s| s.get(key))
            .or_else(|| conf_section.and_then(|s| s.get(key)))
            .cloned()
    };

    let mut opts = HashMap::new();
    match (lookup("aws_access_key_id"), lookup("aws_secret_access_key")) {
        (Some(id), Some(secret)) => {
            opts.insert("aws_access_key_id".to_string(), id);
            opts.insert("aws_secret_access_key".to_string(), secret);
        }
        _ => {
            let mechanism = [
                "sso_start_url",
                "sso_session",
                "credential_process",
                "role_arn",
            ]
            .iter()
            .find(|k| lookup(k).is_some());
            return Err(match mechanism {
                Some(m) => Error::Credentials(format!(
                    "AWS profile '{profile}' uses {m}, which delta-explain does not resolve. \
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
        opts.insert("aws_session_token".to_string(), token);
    }
    if let Some(region) = lookup("region") {
        opts.insert("region".to_string(), region);
    }
    Ok(opts)
}

/// Read one of the AWS shared files into `profile name -> key -> value`.
/// A missing file is an empty map, not an error: the other file may still
/// hold the profile. In the config file sections are `[profile name]`
/// (except `[default]`); in the credentials file they are plain `[name]`.
fn read_aws_file(
    env_override: &str,
    home_relative: &str,
    config_style: bool,
) -> Result<HashMap<String, HashMap<String, String>>, Error> {
    let path = match std::env::var_os(env_override) {
        Some(p) => PathBuf::from(p),
        None => match std::env::home_dir() {
            Some(home) => home.join(home_relative),
            None => return Ok(HashMap::new()),
        },
    };
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Ok(HashMap::new());
    };
    Ok(parse_ini(&content, config_style))
}

/// Minimal INI parse for the AWS shared-file dialect: `[section]` headers,
/// `key = value` pairs, `#`/`;` comments. Nested `s3 =`-style blocks and
/// other AWS extensions are ignored line by line.
fn parse_ini(content: &str, config_style: bool) -> HashMap<String, HashMap<String, String>> {
    let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut current: Option<String> = None;

    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(header) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            let name = header.trim();
            let name = if config_style {
                name.strip_prefix("profile ").unwrap_or(name).trim()
            } else {
                name
            };
            current = Some(name.to_string());
            sections.entry(name.to_string()).or_default();
            continue;
        }
        if let (Some(section), Some((key, value))) = (&current, line.split_once('=')) {
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim();
            // Skip nested-block openers like `s3 =` and their indented body
            // keys we cannot attribute; only plain `key = value` matters here.
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
