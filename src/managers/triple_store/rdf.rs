//! RDF utilities for N-Quad parsing and manipulation.

use std::collections::HashMap;

/// Extracts the subject from an N-Quad triple.
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
pub(crate) fn extract_subject(triple: &str) -> Option<&str> {
    let triple = triple.trim();
    if triple.starts_with('<') {
        // IRI subject: find the closing '>'
        triple.find('>').map(|end| &triple[..=end])
    } else {
        // Blank node or other format: take first whitespace-delimited token
        triple.split_whitespace().next()
    }
}

/// Groups N-Quads by their subject, sorted alphabetically by subject.
///
/// Each group contains all triples that share the same subject.
/// Groups are sorted alphabetically by subject key to match the JS implementation
/// which uses `groupNquadsBySubject(triples, true)` with sorting enabled.
///
/// # Examples
///
/// ```
/// use triple_store::rdf::group_nquads_by_subject;
///
/// let triples = vec![
///     r#"<http://example.org/s2> <http://example.org/p1> "v3" ."#,
///     r#"<http://example.org/s1> <http://example.org/p1> "v1" ."#,
///     r#"<http://example.org/s1> <http://example.org/p2> "v2" ."#,
/// ];
///
/// let groups = group_nquads_by_subject(&triples);
/// assert_eq!(groups.len(), 2);
/// // Groups are sorted: s1 comes before s2
/// assert_eq!(groups[0].len(), 2); // s1 has 2 triples
/// assert_eq!(groups[1].len(), 1); // s2 has 1 triple
/// ```
pub(crate) fn group_nquads_by_subject<'a>(triples: &[&'a str]) -> Vec<Vec<&'a str>> {
    let mut groups: Vec<Vec<&'a str>> = Vec::new();
    let mut subject_to_index: HashMap<&str, usize> = HashMap::new();

    for triple in triples {
        if let Some(subject) = extract_subject(triple) {
            if let Some(&idx) = subject_to_index.get(subject) {
                groups[idx].push(triple);
            } else {
                let idx = groups.len();
                subject_to_index.insert(subject, idx);
                groups.push(vec![triple]);
            }
        }
    }

    // Sort groups alphabetically by subject to match JS behavior
    // JS calls groupNquadsBySubject with sort=true, which sorts by subject key
    groups.sort_by(|a, b| {
        let subj_a = a.first().and_then(|t| extract_subject(t)).unwrap_or("");
        let subj_b = b.first().and_then(|t| extract_subject(t)).unwrap_or("");
        subj_a.cmp(subj_b)
    });

    groups
}

/// Groups owned N-Quads by their subject.
///
/// Similar to `group_nquads_by_subject` but works with owned strings.
/// Returns groups of cloned strings.
pub(crate) fn group_nquads_by_subject_owned(triples: &[String]) -> Vec<Vec<String>> {
    let refs: Vec<&str> = triples.iter().map(|s| s.as_str()).collect();
    group_nquads_by_subject(&refs)
        .into_iter()
        .map(|group| group.into_iter().map(String::from).collect())
        .collect()
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
/// assert_eq!(extract_quoted_string(triple), Some("hello world".to_string()));
/// ```
pub(crate) fn extract_quoted_string(triple: &str) -> Option<String> {
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
pub(crate) fn extract_quoted_integer(triple: &str) -> Option<u64> {
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
pub(crate) fn extract_datetime_as_unix(triple: &str) -> Option<u64> {
    let datetime_str = extract_quoted_string(triple)?;
    parse_iso_datetime(&datetime_str)
}

/// Parse an ISO 8601 datetime string to unix timestamp.
///
/// Handles formats like "2024-01-15T10:30:00Z" or "2024-01-15T10:30:00.000Z"
pub(crate) fn parse_iso_datetime(s: &str) -> Option<u64> {
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
pub(crate) fn extract_uri_suffix(triple: &str, prefix: &str) -> Option<String> {
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
    fn test_group_nquads_by_subject_simple() {
        let triples = vec![
            r#"<http://example.org/subject1> <http://example.org/predicate1> "value1" ."#,
            r#"<http://example.org/subject1> <http://example.org/predicate2> "value2" ."#,
            r#"<http://example.org/subject2> <http://example.org/predicate1> "value3" ."#,
        ];

        let groups = group_nquads_by_subject(&triples);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2); // subject1 has 2 triples
        assert_eq!(groups[1].len(), 1); // subject2 has 1 triple
        assert!(groups[0][0].contains("subject1"));
        assert!(groups[0][1].contains("subject1"));
        assert!(groups[1][0].contains("subject2"));
    }

    #[test]
    fn test_group_nquads_sorted_alphabetically() {
        // Triples arrive in non-alphabetical order: person/1, person/2, organization/1
        // After sorting: organization/1, person/1, person/2
        let triples = vec![
            r#"<http://example.org/person/1> <http://schema.org/name> "Alice" ."#,
            r#"<http://example.org/person/1> <http://schema.org/age> "30" ."#,
            r#"<http://example.org/person/2> <http://schema.org/name> "Bob" ."#,
            r#"<http://example.org/organization/1> <http://schema.org/name> "Acme Corp" ."#,
            r#"<http://example.org/person/1> <http://schema.org/worksFor> <http://example.org/organization/1> ."#,
        ];

        let groups = group_nquads_by_subject(&triples);

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
    fn test_group_nquads_sorting_matches_js() {
        // Test that sorting matches JS behavior: alphabetical by subject key
        // Input order: z, a, m -> Output order: a, m, z
        let triples = vec![
            r#"<http://example.org/z> <http://example.org/p> "z" ."#,
            r#"<http://example.org/a> <http://example.org/p> "a" ."#,
            r#"<http://example.org/m> <http://example.org/p> "m" ."#,
        ];

        let groups = group_nquads_by_subject(&triples);

        assert_eq!(groups.len(), 3);
        assert!(groups[0][0].contains("/a>"));
        assert!(groups[1][0].contains("/m>"));
        assert!(groups[2][0].contains("/z>"));
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
        let triple =
            r#"<did:dkg:otp/0x123/456> <https://ontology.origintrail.io/dkg/1.0#publishedBy> <did:dkg:publisherKey/0xABCdef123> ."#;
        assert_eq!(
            extract_uri_suffix(triple, "did:dkg:publisherKey/"),
            Some("0xABCdef123".to_string())
        );
    }
}
