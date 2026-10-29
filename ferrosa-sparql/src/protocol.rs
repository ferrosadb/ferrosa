//! Bounded HTTP parameter validation for the SPARQL Protocol 1.2 transport.
//! Correctness: ambiguous operations and unsupported dataset overrides fail before execution.
//! Last revised: 2026-10-27
//! Last changed: Validate cardinality and version across GET, form and direct POST.
use axum::http::{header, HeaderMap, Uri};
pub(crate) type Parameters = Vec<(String, String)>;
/// Decode only the already bounded request query/form body.
pub(crate) fn parameters(bytes: &[u8]) -> Result<Parameters, String> {
    serde_urlencoded::from_bytes(bytes).map_err(|e| format!("invalid protocol parameters: {e}"))
}
pub(crate) fn one<'a>(
    parameters: &'a [(String, String)],
    key: &str,
) -> Result<Option<&'a str>, String> {
    let mut values = parameters
        .iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| v.as_str());
    let value = values.next();
    if values.next().is_some() {
        return Err(format!("duplicate '{key}' parameter"));
    }
    Ok(value)
}
pub(crate) fn validate(parameters: &[(String, String)]) -> Result<(), String> {
    for name in ["query", "update", "version", "keyspace"] {
        one(parameters, name)?;
    }
    if one(parameters, "query")?.is_some() && one(parameters, "update")?.is_some() {
        return Err("a request must contain exactly one query or update".into());
    }
    for (key, _) in parameters {
        if matches!(
            key.as_str(),
            "default-graph-uri" | "named-graph-uri" | "using-graph-uri" | "using-named-graph-uri"
        ) {
            return Err(format!("protocol dataset selection '{key}' is not implemented; use an explicit supported GRAPH pattern"));
        }
    }
    if let Some(version) = one(parameters, "version")? {
        validate_version(version)?;
    }
    Ok(())
}
fn validate_version(version: &str) -> Result<(), String> {
    if matches!(version, "1.1" | "1.2") {
        Ok(())
    } else {
        Err(format!("unsupported SPARQL operation version '{version}'"))
    }
}
/// Validate options that were formerly lost by direct-body request handling.
pub(crate) fn validate_post_options(
    headers: &HeaderMap,
    uri: &Uri,
    body: &[u8],
) -> Result<(), String> {
    let uri_params = parameters(uri.query().unwrap_or("").as_bytes())?;
    validate(&uri_params)?;
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let mut parts = ct.split(';');
    let media = parts.next().unwrap_or("").trim();
    if media.eq_ignore_ascii_case("application/x-www-form-urlencoded") {
        validate(&parameters(body)?)?;
        if !uri_params.is_empty() {
            return Err("form protocol parameters belong in the request body".into());
        }
    }
    let version = media_version(parts)?;
    if let Some(version) = version {
        validate_version(version)?;
    }
    if uri_params
        .iter()
        .any(|(key, _)| matches!(key.as_str(), "query" | "update" | "version"))
    {
        return Err("direct POST uses a body operation and media-type version".into());
    }
    Ok(())
}
/// The request service URI is the fallback base; explicit SPARQL BASE still wins.
/// Host is validated as an IRI, and forwarded-host/proto headers are not trusted.
pub(crate) fn service_base(headers: &HeaderMap, uri: &Uri, scheme: &str) -> Result<String, String> {
    let host = uri
        .authority()
        .map(|a| a.as_str())
        .or_else(|| headers.get(header::HOST).and_then(|v| v.to_str().ok()))
        .ok_or("missing HTTP Host")?;
    let base = format!("{scheme}://{host}{}", uri.path());
    oxrdf::NamedNode::new(base.clone()).map_err(|_| "invalid service URI")?;
    Ok(base)
}

/// A direct request announces its operation version as a media-type parameter.
fn media_version<'a>(parts: impl Iterator<Item = &'a str>) -> Result<Option<&'a str>, String> {
    let mut version = None;
    for part in parts {
        if let Some((key, value)) = part.trim().split_once('=') {
            if key.trim().eq_ignore_ascii_case("version")
                && version.replace(value.trim().trim_matches('"')).is_some()
            {
                return Err("duplicate version media parameter".into());
            }
        }
    }
    Ok(version)
}
