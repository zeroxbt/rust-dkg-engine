//! This module provides tools for parsing and serializing RDF data as N-Quads.
//! It supports parsing from strings to structured data (`Quad`), handling various
//! RDF-related formats and datatypes. It includes utilities for canonicalization
//! of RDF datasets according to the URDNA2015 algorithm.
//!
use crate::error::URDNAError;
use regex::Regex;
use std::collections::HashMap;

const RDF_LANGSTRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

// Regular expressions used to parse N-Quads from textual representations.
// These are compiled once using `lazy_static` for efficiency.
lazy_static::lazy_static! {
    static ref EOLN: Regex = Regex::new(r"(?:\r\n)|(?:\n)|(?:\r)").unwrap();
    static ref EMPTY: Regex = Regex::new(r"^[ \t]*$").unwrap();
    static ref QUAD: Regex = {
        let pn_chars_base = "A-Za-z\u{00C0}-\u{00D6}\u{00D8}-\u{00F6}\u{00F8}-\u{02FF}\u{0370}-\u{037D}\u{037F}-\u{1FFF}\u{200C}-\u{200D}\u{2070}-\u{218F}\u{2C00}-\u{2FEF}\u{3001}-\u{D7FF}\u{F900}-\u{FDCF}\u{FDF0}-\u{FFFD}";

        let pn_chars_u = format!("{}_", pn_chars_base);
        let pn_chars = format!("{}0-9-\u{00B7}\u{0300}-\u{036F}\u{203F}-\u{2040}", pn_chars_u);

        // Define regex patterns
        let blank_node_label = &format!(
            "(_:(?:[{pn_chars_u}0-9])(?:(?:[{pn_chars}.])*(?:[{pn_chars}]))?)",
            pn_chars_u = pn_chars_u,
            pn_chars = pn_chars
        ).to_string();

        let iri = &"(?:<([^:]+:[^>]*)>)".to_string();
        let plain = &"\"([^\"\\\\]*(?:\\\\.[^\"\\\\]*)*)\"".to_string();
        let data_type = &format!("(?:\\^\\^{})", iri);
        let language = &"(?:@([a-zA-Z]+(?:-[a-zA-Z0-9]+)*))".to_string();
        let literal = &format!("(?:{}(?:{}|{})?)", plain, data_type, language);
        let ws = &"[ \\t]+".to_string();
        let wso = &"[ \\t]*".to_string();

        let subject = &format!("(?:{}|{}){}", iri, blank_node_label, ws);
        let property = &format!("{}{}", iri, ws);
        let object = &format!("(?:{}|{}|{}){}", iri, blank_node_label, literal, wso);
        let graph_name = &format!("(?:\\.|(?:(?:{}|{}){}\\.))", iri, blank_node_label, wso);

        Regex::new(&format!(r"^{}{}{}{}{}{}$", wso, subject, property, object, graph_name, wso)).unwrap()
    };
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum TermType {
    NamedNode,
    BlankNode,
    Literal,
    DefaultGraph,
}
/// Represents a term within an RDF Quad.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Term {
    pub term_type: TermType,
    pub value: String,
}

/// Represents a literal value in RDF, including optional datatype and language annotations.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Literal {
    pub datatype: Term,
    pub language: String,
}

/// Represents an RDF Quad, consisting of subject, predicate, object, and graph components.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Quad {
    pub subject: Term,
    pub predicate: Term,
    pub object: Term,
    pub graph: Term,
    pub object_literal: Option<Literal>,
}

impl Quad {
    /// Compares this quad with another for equality, considering potential language and datatype differences.
    fn compare(&self, other: &Self) -> bool {
        if self.subject != other.subject || self.object != other.object {
            return false;
        }

        if self.predicate.value != other.predicate.value {
            return false;
        }

        match (&self.object_literal, &other.object_literal) {
            (Some(lit1), Some(lit2)) => {
                lit1.datatype == lit2.datatype && lit1.language == lit2.language
            }
            (None, None) => true,
            _ => false,
        }
    }
}

/// Provides functionality to parse and serialize N-Quads, handling RDF data according to specified patterns.
pub struct NQuads;

impl NQuads {
    /// Parses a string input into a vector of `Quad` structures, handling errors via the `URDNAError` type.
    pub fn parse(input: &str) -> Result<Vec<Quad>, URDNAError> {
        let mut dataset = Vec::new();
        let mut graphs: HashMap<String, Vec<Quad>> = HashMap::new();

        for (line_number, line) in EOLN.split(input).enumerate() {
            if EMPTY.is_match(line) {
                continue;
            }

            let captures = QUAD.captures(line).ok_or_else(|| {
                URDNAError::Parsing(format!(
                    "Failed to match N-Quad pattern on line {}.",
                    line_number + 1
                ))
            })?;

            let subject = if let Some(iri) = captures.get(1) {
                Term {
                    term_type: TermType::NamedNode,
                    value: iri.as_str().to_string(),
                }
            } else {
                Term {
                    term_type: TermType::BlankNode,
                    value: captures[2].to_string(),
                }
            };

            let predicate = Term {
                term_type: TermType::NamedNode,
                value: captures[3].to_string(),
            };

            let object;
            let object_literal;

            if let Some(iri) = captures.get(4) {
                object = Term {
                    term_type: TermType::NamedNode,
                    value: iri.as_str().to_string(),
                };
                object_literal = None;
            } else if let Some(blank_node_label) = captures.get(5) {
                object = Term {
                    term_type: TermType::BlankNode,
                    value: blank_node_label.as_str().to_string(),
                };
                object_literal = None;
            } else {
                let unescaped_value = Self::unescape(&captures[6]);

                object = Term {
                    term_type: TermType::Literal,
                    value: unescaped_value.clone(),
                };

                let datatype_value = if let Some(iri) = captures.get(7) {
                    iri.as_str().to_string()
                } else if captures.get(8).is_some() {
                    RDF_LANGSTRING.to_string()
                } else {
                    XSD_STRING.to_string()
                };

                let language = captures
                    .get(8)
                    .map_or(String::new(), |m| m.as_str().to_string());

                object_literal = Some(Literal {
                    datatype: Term {
                        term_type: TermType::NamedNode,
                        value: datatype_value,
                    },
                    language,
                });
            }

            let graph = if let Some(iri) = captures.get(9) {
                Term {
                    term_type: TermType::NamedNode,
                    value: iri.as_str().to_string(),
                }
            } else if let Some(blank_node_label) = captures.get(10) {
                Term {
                    term_type: TermType::BlankNode,
                    value: blank_node_label.as_str().to_string(),
                }
            } else {
                Term {
                    term_type: TermType::DefaultGraph,
                    value: String::new(),
                }
            };

            let quad = Quad {
                subject,
                predicate,
                object,
                graph,
                object_literal,
            };

            // only add quad if it is unique in its graph
            let entry = graphs.entry(quad.graph.value.clone()).or_default();
            if !entry.iter().any(|existing| quad.compare(existing)) {
                entry.push(quad.clone());
                dataset.push(quad);
            }
        }

        Ok(dataset)
    }

    /// Serializes components of a quad into a single string following the N-Quad serialization format.
    pub fn serialize_quad_components(
        s: &Term,
        p: &Term,
        o: &Term,
        g: &Term,
        o_l: &Option<Literal>,
    ) -> Result<String, URDNAError> {
        let mut nquad = String::new();

        // subject can only be NamedNode or BlankNode
        match s.term_type {
            TermType::NamedNode => nquad.push_str(&format!("<{}>", s.value)),
            TermType::BlankNode => nquad.push_str(&s.value),
            _ => {
                return Err(URDNAError::Serializing(
                    "Subject must be a NamedNode or BlankNode".to_string(),
                ))
            }
        }

        // predicate can only be NamedNode
        if p.term_type == TermType::NamedNode {
            nquad.push_str(&format!(" <{}> ", p.value));
        } else {
            return Err(URDNAError::Serializing(
                "Predicate must be a NamedNode".to_string(),
            ));
        }

        // object is NamedNode, BlankNode, or Literal
        match o.term_type {
            TermType::NamedNode => nquad.push_str(&format!("<{}>", o.value)),
            TermType::BlankNode => nquad.push_str(&o.value),
            TermType::Literal => {
                nquad.push_str(&format!("\"{}\"", Self::escape(&o.value)));
                // Extracting datatype and language from object_literal for literal objects
                // Ensure that your actual code populates object_literal where appropriate
                if let Some(literal) = o_l {
                    if literal.datatype.value == RDF_LANGSTRING {
                        if !literal.language.is_empty() {
                            nquad.push_str(&format!("@{}", literal.language));
                        }
                    } else if literal.datatype.value != XSD_STRING {
                        nquad.push_str(&format!("^^<{}>", literal.datatype.value));
                    }
                }
            }
            TermType::DefaultGraph => {
                return Err(URDNAError::Serializing(
                    "Object must be a NamedNode, BlankNode, or Literal".to_string(),
                ));
            }
        }

        // graph can only be NamedNode or BlankNode (or DefaultGraph, but that
        // does not add to `nquad`)
        match g.term_type {
            TermType::NamedNode => nquad.push_str(&format!(" <{}>", g.value)),
            TermType::BlankNode => nquad.push_str(&format!(" {}", g.value)),
            TermType::DefaultGraph => {}
            _ => {
                return Err(URDNAError::Serializing(
                    "Graph must be a NamedNode, BlankNode, or DefaultGraph".to_string(),
                ))
            }
        }

        nquad.push_str(" .\n");
        Ok(nquad)
    }

    /// Escapes special characters in a string to ensure valid N-Quad serialization.
    fn escape(s: &str) -> String {
        // Check if the string has any characters that need to be escaped; if not, return it as is
        if !["\"", "\\", "\n", "\r"].iter().any(|&c| s.contains(c)) {
            return s.to_string();
        }

        // Replace matched characters with their escaped counterparts
        let mut escaped = String::with_capacity(s.len());
        for ch in s.chars() {
            match ch {
                '\"' => escaped.push_str("\\\""),
                '\\' => escaped.push_str("\\\\"),
                '\n' => escaped.push_str("\\n"),
                '\r' => escaped.push_str("\\r"),
                _ => escaped.push(ch),
            }
        }
        escaped
    }

    /// Unescapes special characters from a string, typically used when parsing N-Quad data.
    fn unescape(s: &str) -> String {
        // Define regular expression
        let unescape_regex =
            Regex::new(r#"(?:\\([tbnrf"'\\]))|(?:\\u([0-9A-Fa-f]{4}))|(?:\\U([0-9A-Fa-f]{8}))"#)
                .unwrap();

        // Check if the string has any matches; if not, return it as is
        if !unescape_regex.is_match(s) {
            return s.to_string();
        }

        // Replace matched patterns using a closure
        unescape_regex
            .replace_all(s, |caps: &regex::Captures| {
                if let Some(escaped) = caps.get(1) {
                    let s = match escaped.as_str() {
                        "t" => "\t",
                        "b" => r"\b",
                        "n" => "\n",
                        "r" => "\r",
                        "f" => r"\f",
                        "\"" => "\"",
                        "'" => "'",
                        "\\" => "\\",
                        _ => panic!("Unexpected escape sequence"),
                    }
                    .to_owned();

                    s
                } else if let Some(hex) = caps.get(2) {
                    let u = u32::from_str_radix(hex.as_str(), 16).expect("Failed to parse hex");
                    char::from_u32(u).unwrap().to_string()
                } else if caps.get(3).is_some() {
                    // If you need support for \U escape sequences, you should handle it here
                    panic!("Unsupported U escape");
                } else {
                    panic!("Unexpected match without capture");
                }
                .to_string()
            })
            .into_owned()
    }
}
