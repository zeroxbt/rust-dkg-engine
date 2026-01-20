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

/// Groups N-Quads by their subject.
///
/// Each group contains all triples that share the same subject.
/// Maintains insertion order of first occurrence of each subject.
///
/// # Examples
///
/// ```
/// use triple_store::rdf::group_nquads_by_subject;
///
/// let triples = vec![
///     r#"<http://example.org/s1> <http://example.org/p1> "v1" ."#,
///     r#"<http://example.org/s1> <http://example.org/p2> "v2" ."#,
///     r#"<http://example.org/s2> <http://example.org/p1> "v3" ."#,
/// ];
///
/// let groups = group_nquads_by_subject(&triples);
/// assert_eq!(groups.len(), 2);
/// assert_eq!(groups[0].len(), 2); // s1 has 2 triples
/// assert_eq!(groups[1].len(), 1); // s2 has 1 triple
/// ```
pub fn group_nquads_by_subject<'a>(triples: &[&'a str]) -> Vec<Vec<&'a str>> {
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

    groups
}

/// Groups owned N-Quads by their subject.
///
/// Similar to `group_nquads_by_subject` but works with owned strings.
/// Returns groups of cloned strings.
pub fn group_nquads_by_subject_owned(triples: &[String]) -> Vec<Vec<String>> {
    let refs: Vec<&str> = triples.iter().map(|s| s.as_str()).collect();
    group_nquads_by_subject(&refs)
        .into_iter()
        .map(|group| group.into_iter().map(String::from).collect())
        .collect()
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
    fn test_group_nquads_maintains_insertion_order() {
        let triples = vec![
            r#"<http://example.org/person/1> <http://schema.org/name> "Alice" ."#,
            r#"<http://example.org/person/1> <http://schema.org/age> "30" ."#,
            r#"<http://example.org/person/2> <http://schema.org/name> "Bob" ."#,
            r#"<http://example.org/organization/1> <http://schema.org/name> "Acme Corp" ."#,
            r#"<http://example.org/person/1> <http://schema.org/worksFor> <http://example.org/organization/1> ."#,
        ];

        let groups = group_nquads_by_subject(&triples);

        // Should have 3 groups: person/1, person/2, organization/1
        assert_eq!(groups.len(), 3);

        // First group (person/1) should have 3 triples
        assert_eq!(groups[0].len(), 3);
        assert!(groups[0][0].contains("person/1"));
        assert!(groups[0][1].contains("person/1"));
        assert!(groups[0][2].contains("person/1"));

        // Second group (person/2) should have 1 triple
        assert_eq!(groups[1].len(), 1);
        assert!(groups[1][0].contains("person/2"));

        // Third group (organization/1) should have 1 triple
        assert_eq!(groups[2].len(), 1);
        assert!(groups[2][0].contains("organization/1"));
    }
}
