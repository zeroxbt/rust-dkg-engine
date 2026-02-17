//! RDF utilities for line-based triples (N-Triples/N-Quads).

use std::{cmp::Ordering, collections::HashMap};

use dkg_domain::KnowledgeCollectionMetadata;
use oxigraph::{
    io::{RdfFormat, RdfParser, RdfSerializer},
    model::{NamedOrBlankNode, Quad},
};

use super::query::predicates;

/// Extracts the subject from an RDF line (N-Triples/N-Quads).
///
/// Assumes the triple starts with `<subject>` (IRI) or `_:` (blank node).
///
/// # Examples
///
/// ```
/// use triple_store::rdf::extract_subject;
///
/// let triple = r#"<http://example.org/subject> <http://example.org/pred> "value" ."#;
/// assert_eq!(
///     extract_subject(triple),
///     Some("<http://example.org/subject>")
/// );
///
/// let blank = r#"_:b0 <http://example.org/pred> "value" ."#;
/// assert_eq!(extract_subject(blank), Some("_:b0"));
/// ```
pub fn extract_subject(triple: &str) -> Option<&str> {
    let triple = triple.trim();
    if triple.starts_with('<') {
        // IRI subject: find the closing '>'
        triple.find('>').map(|end| &triple[..=end])
    } else {
        // Blank node or other format: take first whitespace-delimited token
        triple.split_whitespace().next()
    }
}

/// Groups RDF lines by parsed subject using an RDF parser+serializer roundtrip.
///
/// This is closer to JS `groupNquadsBySubject` behavior, which parses RDF quads
/// and then serializes each quad back before sorting/hashing.
pub fn group_triples_by_subject(triples: &[String]) -> Result<Vec<Vec<String>>, String> {
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    let mut subject_to_index: HashMap<String, usize> = HashMap::new();
    let parser = RdfParser::from_format(RdfFormat::NQuads).lenient();
    let joined = triples.join("\n");

    for parsed in parser.for_reader(joined.as_bytes()) {
        let quad = parsed.map_err(|e| format!("Failed to parse triple for grouping: {}", e))?;
        let subject_key = subject_key_from_quad(&quad);
        let quad_string = serialize_quad_nquads(&quad)?;

        if let Some(&idx) = subject_to_index.get(&subject_key) {
            groups[idx].1.push(quad_string);
        } else {
            let idx = groups.len();
            subject_to_index.insert(subject_key.clone(), idx);
            groups.push((subject_key, vec![quad_string]));
        }
    }

    groups.sort_by(|a, b| compare_subject_keys_js_locale_like(a.0.as_str(), b.0.as_str()));
    Ok(groups.into_iter().map(|(_, quads)| quads).collect())
}

fn subject_key_from_quad(quad: &Quad) -> String {
    match &quad.subject {
        NamedOrBlankNode::NamedNode(subject) => format!("<{}>", subject.as_str()),
        NamedOrBlankNode::BlankNode(subject) => format!("<{}>", subject.as_str()),
    }
}

fn serialize_quad_nquads(quad: &Quad) -> Result<String, String> {
    let mut serializer = RdfSerializer::from_format(RdfFormat::NQuads).for_writer(Vec::new());
    serializer
        .serialize_quad(quad)
        .map_err(|e| format!("Failed to serialize quad: {}", e))?;
    let bytes = serializer
        .finish()
        .map_err(|e| format!("Failed to finish quad serialization: {}", e))?;
    let text = String::from_utf8(bytes)
        .map_err(|e| format!("Serialized quad is not valid UTF-8: {}", e))?;
    Ok(text.trim_end_matches(['\n', '\r']).to_string())
}

pub fn compare_js_default_string_order(a: &str, b: &str) -> Ordering {
    let mut a_units = a.encode_utf16();
    let mut b_units = b.encode_utf16();

    loop {
        match (a_units.next(), b_units.next()) {
            (Some(au), Some(bu)) => match au.cmp(&bu) {
                Ordering::Equal => {}
                ord => return ord,
            },
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (None, None) => return Ordering::Equal,
        }
    }
}

fn compare_subject_keys_js_locale_like(a: &str, b: &str) -> Ordering {
    let mut a_units = a.encode_utf16();
    let mut b_units = b.encode_utf16();

    loop {
        match (a_units.next(), b_units.next()) {
            (Some(au), Some(bu)) => {
                let al = ascii_lower_u16(au);
                let bl = ascii_lower_u16(bu);

                if al == bl {
                    continue;
                }

                let a_class = ascii_sort_class(al);
                let b_class = ascii_sort_class(bl);
                match a_class.cmp(&b_class) {
                    Ordering::Equal => match al.cmp(&bl) {
                        Ordering::Equal => match au.cmp(&bu) {
                            Ordering::Equal => {}
                            ord => return ord,
                        },
                        ord => return ord,
                    },
                    ord => return ord,
                }
            }
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (None, None) => return Ordering::Equal,
        }
    }
}

fn ascii_sort_class(code_unit: u16) -> u8 {
    if is_ascii_punctuation(code_unit) {
        0
    } else if is_ascii_digit(code_unit) {
        1
    } else if is_ascii_alpha(code_unit) {
        2
    } else {
        3
    }
}

fn ascii_lower_u16(code_unit: u16) -> u16 {
    if is_ascii_upper(code_unit) {
        code_unit + 32
    } else {
        code_unit
    }
}

fn is_ascii_upper(code_unit: u16) -> bool {
    (b'A' as u16..=b'Z' as u16).contains(&code_unit)
}

fn is_ascii_alpha(code_unit: u16) -> bool {
    (b'A' as u16..=b'Z' as u16).contains(&code_unit)
        || (b'a' as u16..=b'z' as u16).contains(&code_unit)
}

fn is_ascii_digit(code_unit: u16) -> bool {
    (b'0' as u16..=b'9' as u16).contains(&code_unit)
}

fn is_ascii_punctuation(code_unit: u16) -> bool {
    matches!(
        code_unit,
        0x21..=0x2F | 0x3A..=0x40 | 0x5B..=0x60 | 0x7B..=0x7E
    )
}

/// Extract a quoted string value from an RDF triple.
///
/// Handles format: `<subject> <predicate> "value" .`
/// Returns the content between the last pair of double quotes.
///
/// # Examples
///
/// ```
/// let triple = r#"<http://example.org/s> <http://example.org/p> "hello world" ."#;
/// assert_eq!(
///     extract_quoted_string(triple),
///     Some("hello world".to_string())
/// );
/// ```
pub fn extract_quoted_string(triple: &str) -> Option<String> {
    // Find the last quote (closing quote of the value)
    let end = triple.rfind('"')?;
    // Find the opening quote
    let before_end = &triple[..end];
    let start = before_end.rfind('"')?;
    let value = &triple[start + 1..end];
    if !value.is_empty() {
        Some(value.to_string())
    } else {
        None
    }
}

/// Extract an integer from a quoted string in an RDF triple.
///
/// Handles format: `<subject> <predicate> "12345" .`
///
/// # Examples
///
/// ```
/// let triple = r#"<http://example.org/s> <http://example.org/p> "12345" ."#;
/// assert_eq!(extract_quoted_integer(triple), Some(12345));
/// ```
pub fn extract_quoted_integer(triple: &str) -> Option<u64> {
    extract_quoted_string(triple)?.parse().ok()
}

/// Extract a datetime value from an RDF triple and convert to unix timestamp.
///
/// Handles format: `<subject> <predicate> "2024-01-15T10:30:00Z"^^<xsd:dateTime> .`
///
/// # Examples
///
/// ```
/// let triple = r#"<http://example.org/s> <http://example.org/p> "2024-01-15T10:30:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."#;
/// assert_eq!(extract_datetime_as_unix(triple), Some(1705315800));
/// ```
pub fn extract_datetime_as_unix(triple: &str) -> Option<u64> {
    let datetime_str = extract_quoted_string(triple)?;
    parse_iso_datetime(&datetime_str)
}

/// Parse an ISO 8601 datetime string to unix timestamp.
///
/// Handles formats like "2024-01-15T10:30:00Z" or "2024-01-15T10:30:00.000Z"
pub fn parse_iso_datetime(s: &str) -> Option<u64> {
    use chrono::{DateTime, Utc};
    // Try parsing as RFC 3339 (which includes ISO 8601 with timezone)
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp() as u64);
    }
    // Try parsing as UTC datetime without timezone suffix
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Some(dt.timestamp() as u64);
    }
    None
}

/// Extract a URI value from an RDF triple that uses a specific prefix.
///
/// For example, to extract the address from:
/// `<ual> <publishedBy> <did:dkg:publisherKey/0x123...> .`
///
/// Call with `prefix = "did:dkg:publisherKey/"` to get `"0x123..."`.
pub fn extract_uri_suffix(triple: &str, prefix: &str) -> Option<String> {
    let prefix_pos = triple.find(prefix)?;
    let after_prefix = &triple[prefix_pos + prefix.len()..];
    // URI ends at '>' or whitespace
    let end_pos = after_prefix.find(['>', ' ', '\t'])?;
    let value = &after_prefix[..end_pos];
    if !value.is_empty() {
        Some(value.to_string())
    } else {
        None
    }
}

/// Parse metadata from network RDF triples.
///
/// Returns KnowledgeCollectionMetadata if all required fields are found, None otherwise.
pub fn parse_metadata_from_triples(triples: &[String]) -> Option<KnowledgeCollectionMetadata> {
    let mut publisher_address: Option<String> = None;
    let mut block_number: Option<u64> = None;
    let mut transaction_hash: Option<String> = None;
    let mut block_timestamp: Option<u64> = None;

    for triple in triples {
        if triple.contains(predicates::PUBLISHED_BY) {
            publisher_address = extract_uri_suffix(triple, predicates::PUBLISHER_KEY_PREFIX);
        } else if triple.contains(predicates::PUBLISHED_AT_BLOCK) {
            block_number = extract_quoted_integer(triple);
        } else if triple.contains(predicates::PUBLISH_TX) {
            transaction_hash = extract_quoted_string(triple);
        } else if triple.contains(predicates::BLOCK_TIME) {
            block_timestamp = extract_datetime_as_unix(triple);
        }
    }

    match (
        publisher_address,
        block_number,
        transaction_hash,
        block_timestamp,
    ) {
        (Some(publisher), Some(block), Some(tx_hash), Some(timestamp)) => Some(
            KnowledgeCollectionMetadata::new(publisher.to_lowercase(), block, tx_hash, timestamp),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_subject_iri() {
        let triple = r#"<http://example.org/subject1> <http://example.org/predicate1> "value1" ."#;
        assert_eq!(
            extract_subject(triple),
            Some("<http://example.org/subject1>")
        );
    }

    #[test]
    fn test_extract_subject_with_whitespace() {
        let triple =
            r#"  <http://example.org/subject1> <http://example.org/predicate1> "value1" ."#;
        assert_eq!(
            extract_subject(triple),
            Some("<http://example.org/subject1>")
        );
    }

    #[test]
    fn test_extract_subject_blank_node() {
        let triple = r#"_:b0 <http://example.org/predicate1> "value1" ."#;
        assert_eq!(extract_subject(triple), Some("_:b0"));
    }

    #[test]
    fn test_group_triples_by_subject_simple() {
        let triples = vec![
            r#"<http://example.org/subject1> <http://example.org/predicate1> "value1" ."#
                .to_string(),
            r#"<http://example.org/subject1> <http://example.org/predicate2> "value2" ."#
                .to_string(),
            r#"<http://example.org/subject2> <http://example.org/predicate1> "value3" ."#
                .to_string(),
        ];

        let groups = group_triples_by_subject(&triples)
            .expect("Expected grouping to succeed for simple triples");

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2); // subject1 has 2 triples
        assert_eq!(groups[1].len(), 1); // subject2 has 1 triple
        assert!(groups[0][0].contains("subject1"));
        assert!(groups[0][1].contains("subject1"));
        assert!(groups[1][0].contains("subject2"));
    }

    #[test]
    fn test_group_triples_sorted_alphabetically() {
        // Triples arrive in non-alphabetical order: person/1, person/2, organization/1
        // After sorting: organization/1, person/1, person/2
        let triples = vec![
            r#"<http://example.org/person/1> <http://schema.org/name> "Alice" ."#.to_string(),
            r#"<http://example.org/person/1> <http://schema.org/age> "30" ."#.to_string(),
            r#"<http://example.org/person/2> <http://schema.org/name> "Bob" ."#.to_string(),
            r#"<http://example.org/organization/1> <http://schema.org/name> "Acme Corp" ."#
                .to_string(),
            r#"<http://example.org/person/1> <http://schema.org/worksFor> <http://example.org/organization/1> ."#
                .to_string(),
        ];

        let groups = group_triples_by_subject(&triples)
            .expect("Expected grouping to succeed for sorted alphabetical test");

        // Should have 3 groups, sorted alphabetically by subject
        assert_eq!(groups.len(), 3);

        // First group (organization/1) should have 1 triple (comes first alphabetically)
        assert_eq!(groups[0].len(), 1);
        assert!(groups[0][0].contains("organization/1"));

        // Second group (person/1) should have 3 triples
        assert_eq!(groups[1].len(), 3);
        assert!(groups[1][0].contains("person/1"));
        assert!(groups[1][1].contains("person/1"));
        assert!(groups[1][2].contains("person/1"));

        // Third group (person/2) should have 1 triple
        assert_eq!(groups[2].len(), 1);
        assert!(groups[2][0].contains("person/2"));
    }

    #[test]
    fn test_group_triples_sorting_matches_js() {
        // Test that sorting matches JS behavior: alphabetical by subject key
        // Input order: z, a, m -> Output order: a, m, z
        let triples = vec![
            r#"<http://example.org/z> <http://example.org/p> "z" ."#.to_string(),
            r#"<http://example.org/a> <http://example.org/p> "a" ."#.to_string(),
            r#"<http://example.org/m> <http://example.org/p> "m" ."#.to_string(),
        ];

        let groups = group_triples_by_subject(&triples)
            .expect("Expected grouping to succeed for JS sort match test");

        assert_eq!(groups.len(), 3);
        assert!(groups[0][0].contains("/a>"));
        assert!(groups[1][0].contains("/m>"));
        assert!(groups[2][0].contains("/z>"));
    }

    #[test]
    fn test_group_triples_sorting_case_matches_js_locale_compare() {
        // JS localeCompare sorts "<uuid:app...>" before "<uuid:D...>".
        let triples = vec![
            r#"<uuid:DzyraSwarm> <http://schema.org/name> "D.Zyra's Input" ."#.to_string(),
            r#"<uuid:appreciation> <http://schema.org/name> "Appreciation" ."#.to_string(),
        ];

        let groups = group_triples_by_subject(&triples)
            .expect("Expected grouping to succeed for case ordering test");
        assert_eq!(groups.len(), 2);
        assert!(groups[0][0].contains("<uuid:appreciation>"));
        assert!(groups[1][0].contains("<uuid:DzyraSwarm>"));
    }

    #[test]
    fn test_group_triples_sorting_punctuation_before_digits_matches_js() {
        // JS localeCompare sorts "<uuid:gemini:2.0:flash>" before "<uuid:gemini2.0flash>".
        let triples = vec![
            r#"<uuid:gemini2.0flash> <http://schema.org/name> "Gemini 2.0 Flash" ."#.to_string(),
            r#"<uuid:gemini:2.0:flash> <http://schema.org/name> "Gemini 2.0 Flash" ."#.to_string(),
        ];

        let groups = group_triples_by_subject(&triples)
            .expect("Expected grouping to succeed for punctuation ordering test");
        assert_eq!(groups.len(), 2);
        assert!(groups[0][0].contains("<uuid:gemini:2.0:flash>"));
        assert!(groups[1][0].contains("<uuid:gemini2.0flash>"));
    }

    #[test]
    fn test_compare_js_default_string_order_uses_utf16_code_units() {
        // JS default Array.sort compares UTF-16 code units. This differs from Rust's
        // scalar-value ordering for some non-BMP vs BMP cases.
        let astral = "a\u{10000}";
        let bmp_private_use = "a\u{E000}";
        assert_eq!(
            compare_js_default_string_order(astral, bmp_private_use),
            Ordering::Less
        );
    }

    #[test]
    fn test_extract_quoted_string() {
        let triple = r#"<http://example.org/s> <http://example.org/p> "hello world" ."#;
        assert_eq!(
            extract_quoted_string(triple),
            Some("hello world".to_string())
        );
    }

    #[test]
    fn test_extract_quoted_string_with_type() {
        // RDF literal with datatype
        let triple = r#"<http://example.org/s> <http://example.org/p> "2024-01-15T10:30:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."#;
        assert_eq!(
            extract_quoted_string(triple),
            Some("2024-01-15T10:30:00Z".to_string())
        );
    }

    #[test]
    fn test_extract_quoted_integer() {
        let triple = r#"<http://example.org/s> <http://example.org/p> "12345" ."#;
        assert_eq!(extract_quoted_integer(triple), Some(12345));
    }

    #[test]
    fn test_extract_datetime_as_unix() {
        let triple = r#"<http://example.org/s> <http://example.org/p> "2024-01-15T10:30:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."#;
        assert_eq!(extract_datetime_as_unix(triple), Some(1705314600));
    }

    #[test]
    fn test_parse_iso_datetime() {
        assert_eq!(parse_iso_datetime("2024-01-15T10:30:00Z"), Some(1705314600));
        assert_eq!(
            parse_iso_datetime("2024-01-15T10:30:00.000Z"),
            Some(1705314600)
        );
    }

    #[test]
    fn test_extract_uri_suffix() {
        let triple = r#"<did:dkg:otp/0x123/456> <https://ontology.origintrail.io/dkg/1.0#publishedBy> <did:dkg:publisherKey/0xABCdef123> ."#;
        assert_eq!(
            extract_uri_suffix(triple, "did:dkg:publisherKey/"),
            Some("0xABCdef123".to_string())
        );
    }
}
