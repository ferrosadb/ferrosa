//! SPARQL result serialization.
//!
//! Converts internal binding sets to SPARQL JSON Results format
//! (`application/sparql-results+json`) per the W3C spec.
//! Correctness: legacy RDF identity tests and COUNT/JSON/protocol regressions.
//! Last revised: 2026-10-27
//! Last changed: Add recursive RDF 1.2 JSON terms without changing scalar storage identity.

use serde::Serialize;
use std::collections::HashMap;

/// Initial text direction for an RDF 1.2 language-tagged literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TextDirection {
    Ltr,
    Rtl,
}

/// A recursive RDF triple term. Only the object may itself be a triple term.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct TripleBinding {
    pub subject: Binding,
    pub predicate: Binding,
    pub object: Binding,
}

/// A typed result binding: recursive objects never pass through JSON strings.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(tag = "type")]
pub enum Binding {
    #[serde(rename = "uri")]
    Uri { value: String },
    #[serde(rename = "bnode")]
    BlankNode {
        #[serde(serialize_with = "serialize_blank_label")]
        value: String,
    },
    #[serde(rename = "literal")]
    Literal {
        value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        datatype: Option<String>,
        #[serde(rename = "xml:lang", skip_serializing_if = "Option::is_none")]
        lang: Option<String>,
        #[serde(rename = "its:dir", skip_serializing_if = "Option::is_none")]
        direction: Option<TextDirection>,
    },
    #[serde(rename = "triple")]
    Triple { value: Box<TripleBinding> },
}

fn serialize_blank_label<S: serde::Serializer>(
    value: &str,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(value.strip_prefix("_:").unwrap_or(value))
}

/// Borrowed fields of a scalar term accepted by the unchanged persisted v2 codec.
type ScalarParts<'a> = (&'a str, &'a str, Option<&'a str>, Option<&'a str>);

impl Binding {
    /// Construct an IRI binding.
    pub fn uri(value: impl Into<String>) -> Self {
        Self::Uri {
            value: value.into(),
        }
    }
    /// Construct a blank node, removing the storage marker from its JSON label.
    pub fn blank_node(value: impl Into<String>) -> Self {
        let value = value.into();
        Self::BlankNode { value }
    }
    /// Construct a scalar literal; language metadata implies its RDF datatype.
    pub fn literal(
        value: impl Into<String>,
        datatype: Option<String>,
        lang: Option<String>,
    ) -> Self {
        Self::Literal {
            value: value.into(),
            datatype: if lang.is_some() { None } else { datatype },
            lang,
            direction: None,
        }
    }
    /// Preserve language and base direction from the parser's typed literal.
    pub fn from_literal(literal: &oxrdf::Literal) -> Self {
        let direction = literal.direction().map(|d| match d {
            oxrdf::BaseDirection::Ltr => TextDirection::Ltr,
            oxrdf::BaseDirection::Rtl => TextDirection::Rtl,
        });
        Self::Literal {
            value: literal.value().to_owned(),
            datatype: literal
                .language()
                .is_none()
                .then(|| literal.datatype().as_str().to_owned()),
            lang: literal.language().map(str::to_owned),
            direction,
        }
    }
    /// Construct a well-formed triple term, or an expression error for bad positions.
    pub fn triple(subject: Self, predicate: Self, object: Self) -> Option<Self> {
        if !matches!(subject, Self::Uri { .. } | Self::BlankNode { .. })
            || !matches!(predicate, Self::Uri { .. })
        {
            return None;
        }
        Some(Self::Triple {
            value: Box::new(TripleBinding {
                subject,
                predicate,
                object,
            }),
        })
    }
    /// The standard JSON term tag, without allocating a per-binding type string.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Uri { .. } => "uri",
            Self::BlankNode { .. } => "bnode",
            Self::Literal { .. } => "literal",
            Self::Triple { .. } => "triple",
        }
    }
    /// Scalar lexical value; triple terms deliberately have no string-valued payload.
    pub fn scalar_value(&self) -> Option<&str> {
        match self {
            Self::Uri { value } | Self::BlankNode { value } | Self::Literal { value, .. } => {
                Some(value)
            }
            Self::Triple { .. } => None,
        }
    }
    /// Effective RDF datatype of a literal.
    pub fn datatype(&self) -> Option<&str> {
        match self {
            Self::Literal {
                direction: Some(_), ..
            } => Some("http://www.w3.org/1999/02/22-rdf-syntax-ns#dirLangString"),
            Self::Literal { lang: Some(_), .. } => {
                Some("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")
            }
            Self::Literal { datatype, .. } => datatype.as_deref(),
            _ => None,
        }
    }
    /// Language tag, when present.
    pub fn lang(&self) -> Option<&str> {
        match self {
            Self::Literal { lang, .. } => lang.as_deref(),
            _ => None,
        }
    }
    /// Borrow the scalar fields supported by the persisted RDF v2 codec.
    pub(crate) fn storage_parts(&self) -> Result<ScalarParts<'_>, crate::error::SparqlError> {
        if self.requires_json12() {
            return Err(crate::error::SparqlError::Plan(
                "RDF 1.2 triple/directional terms are not supported by stored RDF v2".into(),
            ));
        }
        Ok((
            self.kind(),
            self.scalar_value().expect("scalar binding"),
            self.datatype(),
            self.lang(),
        ))
    }
    /// Whether the binding requires the RDF 1.2 result representation.
    pub fn requires_json12(&self) -> bool {
        matches!(
            self,
            Self::Triple { .. }
                | Self::Literal {
                    direction: Some(_),
                    ..
                }
        )
    }
}

/// SPARQL JSON Results format (W3C).
#[derive(Debug, Serialize)]
pub struct SparqlJsonResults {
    pub head: ResultHead,
    pub results: ResultBody,
}

#[derive(Debug, Serialize)]
pub struct ResultHead {
    pub vars: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ResultBody {
    pub bindings: Vec<HashMap<String, Binding>>,
}

impl SparqlJsonResults {
    /// Create an empty result set with the given variable names.
    pub fn new(vars: Vec<String>) -> Self {
        Self {
            head: ResultHead { vars },
            results: ResultBody {
                bindings: Vec::new(),
            },
        }
    }

    /// Add a row of bindings.
    pub fn add_row(&mut self, row: HashMap<String, Binding>) {
        self.results.bindings.push(row);
    }

    /// Serialize to JSON bytes.
    pub(crate) fn requires_json12(&self) -> bool {
        self.results
            .bindings
            .iter()
            .any(|row| row.values().any(Binding::requires_json12))
    }
    /// Serialize using the minimum result-format version required by its terms.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        self.to_json_version(self.requires_json12().then_some("1.2"))
    }

    /// Serialize an explicitly selected JSON version while borrowing all bindings.
    pub fn to_json_version(&self, version: Option<&str>) -> Result<Vec<u8>, serde_json::Error> {
        #[derive(Serialize)]
        struct Head<'a> {
            vars: &'a [String],
            #[serde(skip_serializing_if = "Option::is_none")]
            version: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct Results<'a> {
            head: Head<'a>,
            results: &'a ResultBody,
        }
        serde_json::to_vec(&Results {
            head: Head {
                vars: &self.head.vars,
                version,
            },
            results: &self.results,
        })
    }
}

/// ASK query result.
#[derive(Debug, Serialize)]
pub struct SparqlAskResult {
    pub head: ResultHead,
    pub boolean: bool,
}

/// Supported SPARQL result content types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultFormat {
    /// `application/sparql-results+json` (default for SELECT/ASK).
    Json,
    /// Explicit RDF 1.2 JSON result representation.
    Json12,
    /// Explicit legacy JSON representation; rejects RDF 1.2 terms.
    Json11,
    /// `application/sparql-results+xml` (SPARQL XML Results Format per W3C).
    Xml,
    /// `text/turtle` (Turtle/N-Triples-like serialization).
    Turtle,
    /// `application/n-triples` (N-Triples format).
    NTriples,
}

impl ResultFormat {
    /// Parse an HTTP Accept header value into a result format.
    ///
    /// Returns the best matching format, defaulting to JSON if no recognized
    /// type is found.
    pub fn from_accept(accept: &str) -> Self {
        // Check for exact or partial matches in priority order.
        if accept.contains("application/sparql-results+xml") {
            Self::Xml
        } else if accept.contains("text/turtle") {
            Self::Turtle
        } else if accept.contains("application/n-triples") {
            Self::NTriples
        } else {
            Self::Json
        }
    }

    /// The Content-Type header value for this format.
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/sparql-results+json",
            Self::Json12 => "application/sparql-results+json; version=1.2",
            Self::Json11 => "application/sparql-results+json; version=1.1",
            Self::Xml => "application/sparql-results+xml",
            Self::Turtle => "text/turtle",
            Self::NTriples => "application/n-triples",
        }
    }
}

impl SparqlJsonResults {
    /// Serialize to SPARQL Results XML format per W3C.
    ///
    /// Produces `application/sparql-results+xml` per the W3C SPARQL 1.1
    /// Query Results XML Format specification.  The output is well-formed XML
    /// with the `<sparql>` root, `<head>` with `<variable>` elements, and
    /// `<results>` with `<result>` / `<binding>` / `<uri>` / `<literal>` /
    /// `<bnode>` leaves.
    pub fn to_xml(&self) -> Result<Vec<u8>, String> {
        let mut buf = String::new();
        buf.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        buf.push_str("<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n");

        // <head>
        buf.push_str("  <head>\n");
        for var in &self.head.vars {
            buf.push_str(&format!("    <variable name=\"{}\"/>\n", xml_escape(var)));
        }
        buf.push_str("  </head>\n");

        // <results>
        buf.push_str("  <results>\n");
        for row in &self.results.bindings {
            buf.push_str("    <result>\n");
            for var in &self.head.vars {
                if let Some(binding) = row.get(var) {
                    buf.push_str(&format!(
                        "      <binding name=\"{}\">{}</binding>\n",
                        xml_escape(var),
                        format_xml_binding(binding)?
                    ));
                }
            }
            buf.push_str("    </result>\n");
        }
        buf.push_str("  </results>\n");
        buf.push_str("</sparql>\n");

        Ok(buf.into_bytes())
    }

    /// Serialize to N-Triples format (one triple per line).
    ///
    /// Produces a simple tabular rendering: each binding row becomes one
    /// line with subject/predicate/object terms in N-Triples syntax.
    pub fn to_ntriples(&self) -> Result<Vec<u8>, String> {
        let mut buf = String::new();
        for row in &self.results.bindings {
            let parts: Vec<String> = self
                .head
                .vars
                .iter()
                .filter_map(|var| row.get(var).map(format_ntriples_term))
                .collect::<Result<_, _>>()?;
            if !parts.is_empty() {
                buf.push_str(&parts.join(" "));
                buf.push_str(" .\n");
            }
        }
        Ok(buf.into_bytes())
    }

    /// Serialize to a simple Turtle-like format.
    ///
    /// Uses the same N-Triples serialization since Turtle is a superset
    /// of N-Triples. Full Turtle grouping/prefixing is deferred.
    pub fn to_turtle(&self) -> Result<Vec<u8>, String> {
        self.to_ntriples()
    }
}

/// Escape a string for inclusion in XML text content or attribute values.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Serialize a single binding value as an XML element leaf.
///
/// Per W3C SPARQL Results XML:
/// - URI → `<uri>iri</uri>`
/// - bnode → `<bnode>label</bnode>`
/// - literal → `<literal [xml:lang] [datatype]>value</literal>`
fn format_xml_binding(binding: &Binding) -> Result<String, String> {
    if binding.requires_json12() {
        return Err("XML RDF 1.2 terms are not implemented".into());
    }
    let value = binding.scalar_value().ok_or("non-scalar XML term")?;
    Ok(match binding {
        Binding::Uri { .. } => format!("<uri>{}</uri>", xml_escape(value)),
        Binding::BlankNode { .. } => format!(
            "<bnode>{}</bnode>",
            xml_escape(value.strip_prefix("_:").unwrap_or(value))
        ),
        Binding::Literal { lang, datatype, .. } => {
            let attr = if let Some(lang) = lang {
                format!(" xml:lang=\"{}\"", xml_escape(lang))
            } else if let Some(dt) = datatype {
                format!(" datatype=\"{}\"", xml_escape(dt))
            } else {
                String::new()
            };
            format!("<literal{attr}>{}</literal>", xml_escape(value))
        }
        Binding::Triple { .. } => return Err("XML triple terms are not implemented".into()),
    })
}

/// Format only the scalar RDF terms supported by this legacy writer.
fn format_ntriples_term(binding: &Binding) -> Result<String, String> {
    if binding.requires_json12() {
        return Err("N-Triples RDF 1.2 terms are not implemented".into());
    }
    let value = binding.scalar_value().ok_or("non-scalar RDF term")?;
    Ok(match binding {
        Binding::Uri { .. } => format!("<{value}>"),
        Binding::BlankNode { .. } => {
            if value.starts_with("_:") {
                value.to_owned()
            } else {
                format!("_:{value}")
            }
        }
        Binding::Literal { lang, datatype, .. } => {
            let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
            if let Some(lang) = lang {
                format!("\"{escaped}\"@{lang}")
            } else if let Some(dt) = datatype {
                format!("\"{escaped}\"^^<{dt}>")
            } else {
                format!("\"{escaped}\"")
            }
        }
        Binding::Triple { .. } => return Err("RDF triple terms are not implemented".into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_results_serialize_to_json() {
        let results = SparqlJsonResults::new(vec!["s".into(), "p".into(), "o".into()]);
        let json = results.to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["head"]["vars"], serde_json::json!(["s", "p", "o"]));
        assert!(parsed["results"]["bindings"].as_array().unwrap().is_empty());
    }

    #[test]
    fn results_with_binding_serialize_correctly() {
        let mut results = SparqlJsonResults::new(vec!["name".into()]);
        let mut row = HashMap::new();
        row.insert(
            "name".into(),
            Binding::literal(
                "Alice",
                Some("http://www.w3.org/2001/XMLSchema#string".into()),
                None,
            ),
        );
        results.add_row(row);
        let json = results.to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["results"]["bindings"][0]["name"]["value"], "Alice");
    }

    // --- Content negotiation: ResultFormat ---

    #[test]
    fn result_format_from_accept_turtle() {
        let fmt = ResultFormat::from_accept("text/turtle");
        assert_eq!(fmt, ResultFormat::Turtle);
        assert_eq!(fmt.content_type(), "text/turtle");
    }

    #[test]
    fn result_format_from_accept_ntriples() {
        let fmt = ResultFormat::from_accept("application/n-triples");
        assert_eq!(fmt, ResultFormat::NTriples);
        assert_eq!(fmt.content_type(), "application/n-triples");
    }

    #[test]
    fn result_format_from_accept_json_default() {
        let fmt = ResultFormat::from_accept("application/sparql-results+json");
        assert_eq!(fmt, ResultFormat::Json);
    }

    #[test]
    fn result_format_from_accept_empty_defaults_json() {
        let fmt = ResultFormat::from_accept("");
        assert_eq!(fmt, ResultFormat::Json);
    }

    #[test]
    fn result_format_from_accept_wildcard_defaults_json() {
        let fmt = ResultFormat::from_accept("*/*");
        assert_eq!(fmt, ResultFormat::Json);
    }

    // --- N-Triples serialization ---

    #[test]
    fn ntriples_serialization_uri_binding() {
        let mut results = SparqlJsonResults::new(vec!["s".into(), "p".into(), "o".into()]);
        let mut row = HashMap::new();
        row.insert("s".into(), Binding::uri("http://ex/alice"));
        row.insert("p".into(), Binding::uri("http://ex/knows"));
        row.insert("o".into(), Binding::uri("http://ex/bob"));
        results.add_row(row);
        let nt = String::from_utf8(results.to_ntriples().unwrap()).unwrap();
        assert!(
            nt.contains("<http://ex/alice>"),
            "subject must be bracketed IRI"
        );
        assert!(
            nt.contains("<http://ex/knows>"),
            "predicate must be bracketed IRI"
        );
        assert!(
            nt.contains("<http://ex/bob>"),
            "object must be bracketed IRI"
        );
        assert!(nt.ends_with(" .\n"), "N-Triples line must end with ' .\\n'");
    }

    #[test]
    fn ntriples_serialization_literal_with_lang() {
        let mut results = SparqlJsonResults::new(vec!["o".into()]);
        let mut row = HashMap::new();
        row.insert(
            "o".into(),
            Binding::literal("Alice", None, Some("en".into())),
        );
        results.add_row(row);
        let nt = String::from_utf8(results.to_ntriples().unwrap()).unwrap();
        assert!(
            nt.contains("\"Alice\"@en"),
            "literal must include language tag"
        );
    }

    #[test]
    fn ntriples_serialization_literal_with_datatype() {
        let mut results = SparqlJsonResults::new(vec!["o".into()]);
        let mut row = HashMap::new();
        row.insert(
            "o".into(),
            Binding::literal(
                "42",
                Some("http://www.w3.org/2001/XMLSchema#integer".into()),
                None,
            ),
        );
        results.add_row(row);
        let nt = String::from_utf8(results.to_ntriples().unwrap()).unwrap();
        assert!(
            nt.contains("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            "literal must include datatype"
        );
    }

    #[test]
    fn format_ntriples_term_bnode() {
        let b = Binding::blank_node("_:b0");
        assert_eq!(format_ntriples_term(&b).unwrap(), "_:b0");
    }
}
