use std::collections::HashSet;

use dkg_domain::KnowledgeAsset;

const MAX_KA_TOKENS_PER_COLLECTION: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PrivateGraphMode {
    None = 0,
    All = 1,
    SparseIds = 2,
    Bitmap = 3,
}

impl PrivateGraphMode {
    pub(super) fn from_raw(raw: u32) -> Option<Self> {
        match raw {
            0 => Some(Self::None),
            1 => Some(Self::All),
            2 => Some(Self::SparseIds),
            3 => Some(Self::Bitmap),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PrivateGraphEncoding {
    pub(super) mode: PrivateGraphMode,
    pub(super) payload: Option<Vec<u8>>,
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
pub(super) enum PrivateGraphPresence {
    None,
    All,
    Sparse(HashSet<u64>),
    Bitmap(Vec<u8>),
}

impl PrivateGraphPresence {
    pub(super) fn from_mode_and_payload(
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

    pub(super) fn has_private_graph(&self, token_id: u64) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::Sparse(ids) => ids.contains(&token_id),
            Self::Bitmap(bits) => {
                if token_id == 0 {
                    return false;
                }
                let bit_index = usize::try_from(token_id - 1).unwrap_or(usize::MAX);
                let byte_index = bit_index / 8;
                if byte_index >= bits.len() {
                    return false;
                }
                let mask = 1u8 << (bit_index % 8);
                bits[byte_index] & mask != 0
            }
        }
    }
}

pub(super) fn encode_private_graph_presence(
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

        if ka
            .private_triples()
            .is_some_and(|triples| !triples.is_empty())
        {
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

pub(super) fn encode_sparse_ids(ids: &[u64]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        payload.extend_from_slice(&id.to_le_bytes());
    }
    payload
}

pub(super) fn decode_sparse_ids(payload: &[u8]) -> Option<HashSet<u64>> {
    if !payload.len().is_multiple_of(8) {
        return None;
    }

    let mut ids = HashSet::with_capacity(payload.len() / 8);
    for chunk in payload.chunks_exact(8) {
        let id = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        ids.insert(id);
    }
    Some(ids)
}

pub(super) fn encode_bitmap(ids: &[u64], max_token_id: u64) -> Vec<u8> {
    let capped = max_token_id.min(MAX_KA_TOKENS_PER_COLLECTION);
    let max_index = usize::try_from(capped).unwrap_or(0);
    let mut bytes = vec![0u8; max_index.div_ceil(8)];

    for id in ids {
        if *id == 0 {
            continue;
        }
        let bit_index = usize::try_from(*id - 1).unwrap_or(usize::MAX);
        let byte_index = bit_index / 8;
        if byte_index >= bytes.len() {
            continue;
        }
        bytes[byte_index] |= 1u8 << (bit_index % 8);
    }

    bytes
}
