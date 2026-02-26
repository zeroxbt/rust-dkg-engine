use std::collections::HashSet;

use dkg_domain::KnowledgeAsset;

use super::encoding::{
    SetEncodingMode, bitmap_contains_with_base, decode_sparse_ids as decode_sparse_ids_shared,
    encode_bitmap_with_base as encode_bitmap_with_base_shared,
    encode_sparse_ids as encode_sparse_ids_shared,
};

const MAX_KA_TOKENS_PER_COLLECTION: u64 = 1_000_000;

pub(crate) type PrivateGraphMode = SetEncodingMode;

#[derive(Debug, Clone)]
pub(crate) struct PrivateGraphEncoding {
    pub(crate) mode: PrivateGraphMode,
    pub(crate) payload: Option<Vec<u8>>,
}

impl PrivateGraphEncoding {
    fn none() -> Self {
        Self {
            mode: PrivateGraphMode::None,
            payload: None,
        }
    }

    fn all() -> Self {
        Self {
            mode: PrivateGraphMode::All,
            payload: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PrivateGraphPresence {
    None,
    All,
    Sparse(HashSet<u64>),
    Bitmap(Vec<u8>),
}

impl PrivateGraphPresence {
    pub(crate) fn from_mode_and_payload(
        mode: PrivateGraphMode,
        payload: Option<&[u8]>,
    ) -> Option<Self> {
        match mode {
            PrivateGraphMode::None => Some(Self::None),
            PrivateGraphMode::All => Some(Self::All),
            PrivateGraphMode::SparseIds => {
                let payload = payload?;
                decode_sparse_ids(payload).map(Self::Sparse)
            }
            PrivateGraphMode::Bitmap => payload.map(|p| Self::Bitmap(p.to_vec())),
        }
    }

    pub(crate) fn has_private_graph(&self, token_id: u64) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::Sparse(ids) => ids.contains(&token_id),
            Self::Bitmap(bits) => bitmap_contains_with_base(bits, token_id, 1),
        }
    }
}

pub(crate) fn encode_private_graph_presence(
    knowledge_assets: &[KnowledgeAsset],
) -> PrivateGraphEncoding {
    if knowledge_assets.is_empty() {
        return PrivateGraphEncoding::none();
    }

    let mut token_ids: Vec<u64> = Vec::with_capacity(knowledge_assets.len());
    let mut private_ids: Vec<u64> = Vec::new();

    for ka in knowledge_assets {
        let Some(token_id) = parse_token_id_from_ka_ual(ka.ual()) else {
            return PrivateGraphEncoding::none();
        };
        token_ids.push(token_id);

        if ka.private_triples().is_some_and(|triples| !triples.is_empty()) {
            private_ids.push(token_id);
        }
    }

    token_ids.sort_unstable();
    token_ids.dedup();
    private_ids.sort_unstable();
    private_ids.dedup();

    let total = token_ids.len();
    let private = private_ids.len();

    if private == 0 {
        return PrivateGraphEncoding::none();
    }
    if private == total {
        return PrivateGraphEncoding::all();
    }
    if private <= 256 {
        return PrivateGraphEncoding {
            mode: PrivateGraphMode::SparseIds,
            payload: Some(encode_sparse_ids(&private_ids)),
        };
    }

    let max_token_id = token_ids.into_iter().max().unwrap_or_default();
    PrivateGraphEncoding {
        mode: PrivateGraphMode::Bitmap,
        payload: Some(encode_bitmap(&private_ids, max_token_id)),
    }
}

fn parse_token_id_from_ka_ual(ka_ual: &str) -> Option<u64> {
    ka_ual.rsplit('/').next()?.parse::<u64>().ok()
}

pub(crate) fn encode_sparse_ids(ids: &[u64]) -> Vec<u8> {
    encode_sparse_ids_shared(ids)
}

pub(crate) fn decode_sparse_ids(payload: &[u8]) -> Option<HashSet<u64>> {
    decode_sparse_ids_shared(payload).map(|ids| ids.into_iter().collect())
}

pub(crate) fn encode_bitmap(ids: &[u64], max_token_id: u64) -> Vec<u8> {
    let capped = max_token_id.min(MAX_KA_TOKENS_PER_COLLECTION);
    let bit_len = usize::try_from(capped).unwrap_or(0);
    encode_bitmap_with_base_shared(1, bit_len, ids)
}
