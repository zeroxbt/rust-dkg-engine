use std::collections::HashMap;

use dkg_domain::{Assertion, KnowledgeAsset};
use dkg_triple_store::{
    PRIVATE_HASH_SUBJECT_PREFIX, error::TripleStoreError, extract_subject, group_triples_by_subject,
};

pub(crate) fn build_knowledge_assets(
    knowledge_collection_ual: &str,
    dataset: &Assertion,
) -> Result<Vec<KnowledgeAsset>, TripleStoreError> {
    let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);
    let normalized_public = normalize_triple_lines(&dataset.public);

    let mut filtered_public: Vec<String> = Vec::new();
    let mut private_hash_triples: Vec<String> = Vec::new();

    for triple in normalized_public {
        if triple.starts_with(&private_hash_prefix) {
            private_hash_triples.push(triple);
        } else {
            filtered_public.push(triple);
        }
    }

    let mut public_ka_triples_grouped =
        group_triples_by_subject(&filtered_public).map_err(|source| {
            TripleStoreError::RdfGrouping {
                context: "public triples",
                source,
            }
        })?;
    public_ka_triples_grouped.extend(group_triples_by_subject(&private_hash_triples).map_err(
        |source| TripleStoreError::RdfGrouping {
            context: "private-hash triples",
            source,
        },
    )?);

    let public_subject_map: HashMap<String, usize> = public_ka_triples_grouped
        .iter()
        .enumerate()
        .filter_map(|(idx, group)| {
            group
                .first()
                .and_then(|triple| extract_subject(triple).map(|subj| (subj.to_string(), idx)))
        })
        .collect();

    let mut knowledge_assets: Vec<KnowledgeAsset> = public_ka_triples_grouped
        .into_iter()
        .enumerate()
        .map(|(i, triples)| {
            let ual = format!("{}/{}", knowledge_collection_ual, i + 1);
            KnowledgeAsset::new(ual, triples)
        })
        .collect();

    if let Some(private_triples) = &dataset.private
        && !private_triples.is_empty()
    {
        let normalized_private = normalize_triple_lines(private_triples);
        let private_ka_triples_grouped =
            group_triples_by_subject(&normalized_private).map_err(|source| {
                TripleStoreError::RdfGrouping {
                    context: "private triples",
                    source,
                }
            })?;

        for private_group in private_ka_triples_grouped {
            if let Some(first_triple) = private_group.first()
                && let Some(private_subject) = extract_subject(first_triple)
            {
                let matched_idx = if let Some(&idx) = public_subject_map.get(private_subject) {
                    Some(idx)
                } else {
                    let subject_without_brackets = private_subject
                        .trim_start_matches('<')
                        .trim_end_matches('>');
                    let hashed_subject = format!(
                        "<{}{}>",
                        PRIVATE_HASH_SUBJECT_PREFIX,
                        dkg_blockchain::sha256_hex(subject_without_brackets.as_bytes())
                    );
                    public_subject_map.get(hashed_subject.as_str()).copied()
                };

                if let Some(idx) = matched_idx {
                    match knowledge_assets[idx].private_triples.as_mut() {
                        Some(existing) => existing.extend(private_group),
                        None => knowledge_assets[idx].set_private_triples(private_group),
                    }
                }
            }
        }
    }

    Ok(knowledge_assets)
}

fn normalize_triple_lines(triples: &[String]) -> Vec<String> {
    if !triples.iter().any(|entry| entry.contains('\n')) {
        return triples
            .iter()
            .filter(|entry| !entry.is_empty())
            .cloned()
            .collect();
    }

    triples
        .iter()
        .flat_map(|entry| entry.lines())
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::normalize_triple_lines;

    fn normalize_triple_lines_old(triples: &[String]) -> Vec<String> {
        triples
            .iter()
            .flat_map(|entry| entry.lines())
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn test_normalize_triple_lines_matches_previous_logic() {
        let explicit_cases: Vec<Vec<String>> = vec![
            vec![],
            vec!["a".to_string(), "b".to_string()],
            vec!["".to_string(), "x".to_string(), "".to_string()],
            vec!["one\ntwo".to_string(), "three".to_string()],
            vec!["line1\n\nline3".to_string(), "line4\n".to_string()],
            vec!["😄".to_string(), "caf\u{00e9}\ncafe\u{301}".to_string()],
        ];

        for input in explicit_cases {
            let expected = normalize_triple_lines_old(&input);
            let current = normalize_triple_lines(&input);
            assert_eq!(current, expected, "mismatch for input: {:?}", input);
        }

        for size in 0..128 {
            let input: Vec<String> = (0..size)
                .map(|i| match i % 5 {
                    0 => format!("triple-{i}"),
                    1 => format!("line-{i}\nnext-{i}"),
                    2 => String::new(),
                    3 => "x".repeat((i % 16) + 1),
                    _ => format!("unicode-{}-😄", i),
                })
                .collect();

            let expected = normalize_triple_lines_old(&input);
            let current = normalize_triple_lines(&input);
            assert_eq!(current, expected, "generated mismatch for size={size}");
        }
    }
}
