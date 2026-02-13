//! Domain types used across the codebase.
//!
//! This module contains core data structures that are shared between managers,
//! network protocols, controllers, and services.

mod assertion;
mod blockchain_id;
mod kc;
mod paranet;
mod signature;
mod token_ids;
mod ual;
mod visibility;

pub(crate) use assertion::Assertion;
pub(crate) use blockchain_id::BlockchainId;
pub(crate) use kc::{KnowledgeAsset, KnowledgeCollectionMetadata};
pub(crate) use paranet::{AccessPolicy, ParanetKcLocator};
pub(crate) use signature::SignatureComponents;
pub(crate) use token_ids::TokenIds;
pub(crate) use ual::{ParsedUal, derive_ual, parse_ual};
pub(crate) use visibility::Visibility;
