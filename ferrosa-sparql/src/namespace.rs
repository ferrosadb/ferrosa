//! Standard RDF namespace prefix management.

/// Well-known RDF namespace prefixes.
pub const STANDARD_PREFIXES: &[(&str, &str)] = &[
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("foaf", "http://xmlns.com/foaf/0.1/"),
    ("dc", "http://purl.org/dc/elements/1.1/"),
    ("dcterms", "http://purl.org/dc/terms/"),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("prov", "http://www.w3.org/ns/prov#"),
    ("schema", "http://schema.org/"),
];

/// Build a SPARQL prefix header from the standard prefixes.
pub fn standard_prefix_header() -> String {
    STANDARD_PREFIXES
        .iter()
        .map(|(prefix, iri)| format!("PREFIX {prefix}: <{iri}>"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_prefix_header_contains_rdf() {
        let header = standard_prefix_header();
        assert!(header.contains("PREFIX rdf:"));
        assert!(header.contains("PREFIX foaf:"));
    }
}
