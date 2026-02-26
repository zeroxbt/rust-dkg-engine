use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum BurnedMode {
    None = 0,
    All = 1,
    SparseIds = 2,
    Bitmap = 3,
}

impl BurnedMode {
    pub(crate) fn from_raw(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::All),
            2 => Some(Self::SparseIds),
            3 => Some(Self::Bitmap),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BurnedEncoding {
    pub(crate) mode: BurnedMode,
    pub(crate) payload: Vec<u8>,
}

pub(crate) fn encode_burned_ids(start: u64, end: u64, burned: &[u64]) -> BurnedEncoding {
    if burned.is_empty() {
        return BurnedEncoding {
            mode: BurnedMode::None,
            payload: Vec::new(),
        };
    }

    let total = end.saturating_sub(start).saturating_add(1) as usize;
    let burned_set: HashSet<u64> = burned.iter().copied().collect();
    if burned_set.len() >= total {
        return BurnedEncoding {
            mode: BurnedMode::All,
            payload: Vec::new(),
        };
    }

    if burned_set.len() <= 32 {
        let mut ids: Vec<u64> = burned_set.into_iter().collect();
        ids.sort_unstable();
        return BurnedEncoding {
            mode: BurnedMode::SparseIds,
            payload: encode_sparse_ids(&ids),
        };
    }

    BurnedEncoding {
        mode: BurnedMode::Bitmap,
        payload: encode_bitmap(start, end, burned),
    }
}

pub(crate) fn decode_burned_ids(
    mode: BurnedMode,
    payload: &[u8],
    start: u64,
    end: u64,
) -> Option<Vec<u64>> {
    match mode {
        BurnedMode::None => Some(Vec::new()),
        BurnedMode::All => Some((start..=end).collect()),
        BurnedMode::SparseIds => decode_sparse_ids(payload),
        BurnedMode::Bitmap => decode_bitmap(payload, start, end),
    }
}

fn encode_sparse_ids(ids: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    bytes
}

fn decode_sparse_ids(payload: &[u8]) -> Option<Vec<u64>> {
    if !payload.len().is_multiple_of(8) {
        return None;
    }

    let mut ids = Vec::with_capacity(payload.len() / 8);
    for chunk in payload.chunks_exact(8) {
        let mut b = [0_u8; 8];
        b.copy_from_slice(chunk);
        ids.push(u64::from_le_bytes(b));
    }
    Some(ids)
}

fn encode_bitmap(start: u64, end: u64, burned: &[u64]) -> Vec<u8> {
    let bit_len = end.saturating_sub(start).saturating_add(1) as usize;
    let mut bitmap = vec![0_u8; bit_len.div_ceil(8)];
    for token_id in burned {
        if *token_id < start || *token_id > end {
            continue;
        }
        let index = (*token_id - start) as usize;
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        bitmap[byte_idx] |= 1_u8 << bit_idx;
    }
    bitmap
}

fn decode_bitmap(payload: &[u8], start: u64, end: u64) -> Option<Vec<u64>> {
    if end < start {
        return Some(Vec::new());
    }
    let bit_len = end.saturating_sub(start).saturating_add(1) as usize;
    let expected_bytes = bit_len.div_ceil(8);
    if payload.len() != expected_bytes {
        return None;
    }

    let mut out = Vec::new();
    for i in 0..bit_len {
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        if payload[byte_idx] & (1_u8 << bit_idx) != 0 {
            out.push(start + i as u64);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{BurnedMode, decode_burned_ids, encode_burned_ids};

    #[test]
    fn sparse_roundtrip() {
        let encoded = encode_burned_ids(10, 100, &[10, 11, 99]);
        assert_eq!(encoded.mode as u32, BurnedMode::SparseIds as u32);
        let decoded = decode_burned_ids(encoded.mode, &encoded.payload, 10, 100).unwrap();
        assert_eq!(decoded, vec![10, 11, 99]);
    }

    #[test]
    fn bitmap_roundtrip() {
        let burned: Vec<u64> = (1..=128).step_by(2).collect();
        let encoded = encode_burned_ids(1, 128, &burned);
        assert_eq!(encoded.mode as u32, BurnedMode::Bitmap as u32);
        let decoded = decode_burned_ids(encoded.mode, &encoded.payload, 1, 128).unwrap();
        assert_eq!(decoded, burned);
    }

    #[test]
    fn all_mode_roundtrip() {
        let burned: Vec<u64> = (50..=60).collect();
        let encoded = encode_burned_ids(50, 60, &burned);
        assert_eq!(encoded.mode as u32, BurnedMode::All as u32);
        let decoded = decode_burned_ids(encoded.mode, &encoded.payload, 50, 60).unwrap();
        assert_eq!(decoded, burned);
    }
}
