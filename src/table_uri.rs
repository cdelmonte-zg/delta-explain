use url::Url;

use crate::error::{Error, Result};

pub fn parse(path: &str) -> Result<Url> {
    if let Ok(mut url) = Url::parse(path)
        && url.scheme() != "file"
        && url.has_host()
    {
        if !url.path().ends_with('/') {
            let with_slash = format!("{}/", url.path());
            url.set_path(&with_slash);
        }

        return Ok(url);
    }

    let absolute = std::fs::canonicalize(path)
        .map_err(|e| Error::TableUri(format!("Invalid path '{path}': {e}")))?;

    Url::from_directory_path(&absolute)
        .map_err(|_| Error::TableUri(format!("Cannot convert path to URL: {absolute:?}")))
}
