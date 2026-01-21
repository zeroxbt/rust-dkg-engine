use std::collections::{HashMap, HashSet};

/// Provides an implementation of the URDNA2015 algorithm for RDF graph canonicalization.
/// This module includes functions and structures necessary to perform canonicalization of RDF
/// datasets, which is often required for tasks such as graph isomorphism checks and digital
/// signatures.
///
/// The canonicalization process involves assigning new identifiers to blank nodes while
/// preserving the semantic structure of the RDF graph, ensuring consistent serialization
/// across different instances of equivalent graphs.
use async_recursion::async_recursion;

use crate::{
    error::URDNAError,
    identifier_issuer::IdentifierIssuer,
    message_digest::MessageDigest,
    n_quad::{NQuads, Quad, Term, TermType},
    permuter::Permuter,
};

/// Stores information related to a particular blank node during the canonicalization process.
pub struct Info {
    /// A set of quads that include this blank node.
    pub quads: HashSet<Quad>,
    /// The hash value associated with this blank node, if it has been computed.
    pub hash: Option<String>,
}

/// The main structure for the URDNA2015 algorithm, handling the state and operations required for
/// canonicalization.
pub struct URDNA2015 {
    /// A special identifier issuer for managing canonical identifiers for blank nodes.
    canonical_issuer: IdentifierIssuer,
    /// A map of blank node identifiers to their associated information and state.
    blank_node_info: HashMap<String, Info>,
    /// Tracks the depth of recursive hashing operations to prevent infinite recursion.
    deep_iterations: HashMap<String, i32>,
}

impl URDNA2015 {
    /// Constructs a new instance of the `URDNA2015` canonicalizer.
    pub fn new() -> Self {
        Self {
            canonical_issuer: IdentifierIssuer::new("_:c14n".to_string(), None, 0),
            blank_node_info: HashMap::new(),
            deep_iterations: HashMap::new(),
        }
    }

    /// The main function to process the dataset and return a canonicalized string of N-Quads.
    /// It processes each quad in the dataset, updating state for blank nodes and recursively
    /// hashing to resolve complex blank node structures.
    ///
    /// # Arguments
    /// * `dataset` - A vector of `Quad` structures representing the RDF dataset.
    ///
    /// # Returns
    /// Returns a `Result` containing the canonicalized string or an `URDNAError` on failure.
    pub async fn main(&mut self, dataset: Vec<Quad>) -> Result<String, URDNAError> {
        let quads = dataset.clone();

        for quad in dataset {
            self.add_blank_node_quad_info(&quad, quad.subject());
            self.add_blank_node_quad_info(&quad, quad.object());
            self.add_blank_node_quad_info(&quad, quad.graph());
        }

        let mut hash_to_blank_nodes = HashMap::new();
        let non_normalized = self.blank_node_info.keys().cloned().collect::<Vec<_>>();
        for id in non_normalized {
            self.hash_and_track_blank_node(&id, &mut hash_to_blank_nodes)
                .await?;
        }

        let mut hashes = hash_to_blank_nodes.keys().clone().collect::<Vec<_>>();
        hashes.sort();

        let mut non_unique = vec![];
        for hash in hashes {
            let id_list = hash_to_blank_nodes.get(hash).unwrap();
            if id_list.len() > 1 {
                non_unique.push(id_list);
                continue;
            }

            let id = &id_list[0];
            self.canonical_issuer.get_id(Some(id));
        }

        for id_list in non_unique {
            let mut hash_path_list = vec![];

            for id in id_list {
                if self.canonical_issuer.has_id(id) {
                    continue;
                }

                let mut issuer = IdentifierIssuer::new("_:b".to_string(), None, 0);

                issuer.get_id(Some(id));

                let result = self.hash_ndegree_quads(id, &issuer).await;
                hash_path_list.push(result);
            }

            hash_path_list.sort_by(|a, b| a.0.cmp(&b.0));

            for result in hash_path_list {
                let old_ids = result.1.get_old_ids();
                for id in old_ids {
                    self.canonical_issuer.get_id(Some(&id));
                }
            }
        }

        // Note: At this point all blank nodes in the set of RDF quads have been
        // assigned canonical identifiers, which have been stored in the canonical
        // issuer. Here each quad is updated by assigning each of its blank nodes
        // its new identifier.

        let mut normalized = vec![];
        for quad in quads {
            let n_quad = NQuads::serialize_quad_components(
                &self.term_with_canonical_id(quad.subject().clone()),
                quad.predicate(),
                &self.term_with_canonical_id(quad.object().clone()),
                &self.term_with_canonical_id(quad.graph().clone()),
                quad.object_literal(),
            )?;

            normalized.push(n_quad);
        }

        normalized.sort();

        Ok(normalized.join(""))
    }

    /// Performs recursive hashing of N-degree quads associated with a specific blank node.
    /// This method is crucial for ensuring that blank nodes are canonicalized accurately by
    /// considering all related quads and their semantic structure.
    ///
    /// # Arguments
    /// * `id` - The identifier of the blank node being processed.
    /// * `issuer` - An `IdentifierIssuer` instance managing the issuance of new identifiers.
    ///
    /// # Returns
    /// Returns a tuple containing the resulting hash and a possibly updated `IdentifierIssuer`.
    #[async_recursion]
    async fn hash_ndegree_quads(
        &mut self,
        id: &str,
        issuer: &IdentifierIssuer,
    ) -> (String, IdentifierIssuer) {
        let deep_iterations = match self.deep_iterations.get(id) {
            Some(x) => *x,
            None => 0,
        };

        let mut result_issuer = issuer.clone();

        self.deep_iterations
            .insert(id.to_owned(), deep_iterations + 1);

        let mut md = self.create_message_digest();
        let hash_to_related = self.create_hash_to_related(id, &mut result_issuer).await;

        let mut hashes = hash_to_related.keys().clone().collect::<Vec<_>>();
        hashes.sort();
        for hash in hashes {
            md.update(hash);

            let mut chosen_path = String::from("");

            let mut chosen_issuer = result_issuer.clone();

            let mut permuter = Permuter::new(hash_to_related.get(hash).unwrap().clone());

            while let Some(permutation) = permuter.next() {
                let mut issuer_clone = issuer.clone();

                let mut path = String::from("");

                let mut recursion_list = vec![];

                let mut next_permutation = false;
                for related in permutation {
                    if self.canonical_issuer.has_id(&related) {
                        path += &self.canonical_issuer.get_id(Some(&related));
                    } else {
                        if !issuer_clone.has_id(&related) {
                            recursion_list.push(related.clone());
                        }

                        path += &issuer_clone.get_id(Some(&related));
                    }

                    if !chosen_path.is_empty() && path > chosen_path {
                        next_permutation = true;
                        break;
                    }
                }

                if next_permutation {
                    continue;
                }

                for related in recursion_list {
                    let result = self.hash_ndegree_quads(&related, &issuer_clone).await;

                    path += &issuer_clone.get_id(Some(&related));

                    path += &format!("<{}>", result.0);

                    issuer_clone = result.1;

                    if !chosen_path.is_empty() && path > chosen_path {
                        next_permutation = true;
                        break;
                    }
                }

                if next_permutation {
                    continue;
                }

                if chosen_path.is_empty() || path < chosen_path {
                    chosen_path = path.clone();
                    chosen_issuer = issuer_clone.clone();
                }
            }

            md.update(&chosen_path);

            result_issuer = chosen_issuer;
        }

        (md.digest(), result_issuer.clone())
    }

    async fn hash_and_track_blank_node(
        &mut self,
        id: &str,
        hash_to_blank_nodes: &mut HashMap<String, Vec<String>>,
    ) -> Result<(), URDNAError> {
        let hash = self.hash_first_degree_quads(id).await.ok_or_else(|| {
            URDNAError::Hashing(format!(
                "No hash available for first-degree quads of ID: {}",
                id
            ))
        })?;

        if let Some(id_list) = hash_to_blank_nodes.get_mut(&hash) {
            id_list.push(id.to_owned());
        } else {
            hash_to_blank_nodes.insert(hash, vec![id.to_owned()]);
        }

        Ok(())
    }

    async fn hash_first_degree_quads(&mut self, id: &str) -> Option<String> {
        let mut nquads = vec![];

        let info = self.blank_node_info.get(id).unwrap();
        let quads = &info.quads;

        for quad in quads {
            nquads.push(
                NQuads::serialize_quad_components(
                    &self.modify_first_degree_term(id, quad.subject()),
                    quad.predicate(),
                    &self.modify_first_degree_term(id, quad.object()),
                    &self.modify_first_degree_term(id, quad.graph()),
                    quad.object_literal(),
                )
                .unwrap(),
            );
        }

        nquads.sort_unstable();

        let mut md = self.create_message_digest();
        for nquad in nquads {
            md.update(&nquad);
        }
        let info = self.blank_node_info.get_mut(id).unwrap();
        info.hash = Some(md.digest());

        info.hash.clone()
    }

    async fn create_hash_to_related(
        &mut self,
        id: &str,
        issuer: &mut IdentifierIssuer,
    ) -> HashMap<String, Vec<String>> {
        let mut hash_to_related = HashMap::new();

        let quads = self.blank_node_info.get_mut(id).unwrap().quads.clone();

        for quad in quads {
            self.add_related_blank_node_hash(
                &quad,
                quad.subject(),
                "s",
                id,
                issuer,
                &mut hash_to_related,
            )
            .await;
            self.add_related_blank_node_hash(
                &quad,
                quad.object(),
                "o",
                id,
                issuer,
                &mut hash_to_related,
            )
            .await;
            self.add_related_blank_node_hash(
                &quad,
                quad.graph(),
                "g",
                id,
                issuer,
                &mut hash_to_related,
            )
            .await;
        }

        hash_to_related
    }
    async fn add_related_blank_node_hash(
        &mut self,
        quad: &Quad,
        term: &Term,
        position: &str,
        id: &str,
        issuer: &mut IdentifierIssuer,
        hash_to_related: &mut HashMap<String, Vec<String>>,
    ) {
        if !(*term.term_type() == TermType::BlankNode && term.value() != id) {
            return;
        }

        let related = term.value().to_string();
        let hash = self
            .hash_related_blank_node(related.as_str(), quad, issuer, position)
            .await;

        if let Some(entries) = hash_to_related.get_mut(&hash) {
            entries.push(related);
        } else {
            hash_to_related.insert(hash, vec![related]);
        }
    }

    async fn hash_related_blank_node(
        &mut self,
        related: &str,
        quad: &Quad,
        issuer: &mut IdentifierIssuer,
        position: &str,
    ) -> String {
        let id = if self.canonical_issuer.has_id(related) {
            self.canonical_issuer.get_id(Some(related))
        } else if issuer.has_id(related) {
            issuer.get_id(Some(related))
        } else {
            self.blank_node_info
                .get(related)
                .unwrap()
                .hash
                .clone()
                .unwrap()
        };

        let mut md = self.create_message_digest();
        md.update(position);

        if position != "g" {
            md.update(&self.get_related_predicate(quad));
        }

        md.update(id.as_str());

        md.digest()
    }

    fn create_message_digest(&self) -> MessageDigest {
        MessageDigest::new()
    }

    /// Adds quad information to the blank node tracking structure, used in the canonicalization
    /// process.
    fn add_blank_node_quad_info(&mut self, quad: &Quad, term: &Term) {
        if *term.term_type() != TermType::BlankNode {
            return;
        }
        let id = term.value().to_string();
        if let Some(info) = self.blank_node_info.get_mut(&id) {
            info.quads.insert(quad.clone());
        } else {
            self.blank_node_info.insert(
                id,
                Info {
                    quads: HashSet::from_iter([quad.clone()]),
                    hash: None,
                },
            );
        }
    }

    fn term_with_canonical_id(&mut self, term: Term) -> Term {
        if *term.term_type() == TermType::BlankNode
            && !term.value().starts_with(self.canonical_issuer.prefix())
        {
            return Term::new(
                TermType::BlankNode,
                self.canonical_issuer.get_id(Some(term.value())),
            );
        }
        term
    }

    /// Modifies a term representing a first-degree connection to adjust its identifier if
    /// necessary.
    fn modify_first_degree_term(&self, id: &str, term: &Term) -> Term {
        if *term.term_type() != TermType::BlankNode {
            return term.clone();
        }

        Term::new(
            TermType::BlankNode,
            if term.value() == id {
                "_:a".to_owned()
            } else {
                "_:z".to_owned()
            },
        )
    }

    /// Retrieves the predicate as a string from a given quad, formatted for use in hash inputs.
    fn get_related_predicate(&self, quad: &Quad) -> String {
        format!("<{}>", quad.predicate().value())
    }
}
