use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum SetEncodingMode {
    None = 0,
    All = 1,
    SparseIds = 2,
    Bitmap = 3,
}

impl SetEncodingMode {
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
pub(crate) struct SetEncoding {
    pub(crate) mode: SetEncodingMode,
    pub(crate) payload: Vec<u8>,
}

pub(crate) fn encode_set_for_contiguous_range(
    start: u64,
    end: u64,
    ids: &[u64],
    sparse_threshold: usize,
) -> SetEncoding {
    if ids.is_empty() || end < start {
        return SetEncoding {
            mode: SetEncodingMode::None,
            payload: Vec::new(),
        };
    }

    let total = end.saturating_sub(start).saturating_add(1) as usize;
    let ids_set: HashSet<u64> = ids
        .iter()
        .copied()
        .filter(|id| *id >= start && *id <= end)
        .collect();
    if ids_set.is_empty() {
        return SetEncoding {
            mode: SetEncodingMode::None,
            payload: Vec::new(),
        };
    }
    if ids_set.len() >= total {
        return SetEncoding {
            mode: SetEncodingMode::All,
            payload: Vec::new(),
        };
    }

    if ids_set.len() <= sparse_threshold {
        let mut sorted_ids: Vec<u64> = ids_set.into_iter().collect();
        sorted_ids.sort_unstable();
        return SetEncoding {
            mode: SetEncodingMode::SparseIds,
            payload: encode_sparse_ids(&sorted_ids),
        };
    }

    let bit_len = total;
    let mut sorted_ids: Vec<u64> = ids_set.into_iter().collect();
    sorted_ids.sort_unstable();
    SetEncoding {
        mode: SetEncodingMode::Bitmap,
        payload: encode_bitmap_with_base(start, bit_len, &sorted_ids),
    }
}

pub(crate) fn decode_set_for_contiguous_range(
    mode: SetEncodingMode,
    payload: &[u8],
    start: u64,
    end: u64,
) -> Option<Vec<u64>> {
    match mode {
        SetEncodingMode::None => Some(Vec::new()),
        SetEncodingMode::All => {
            if end < start {
                return Some(Vec::new());
            }
            Some((start..=end).collect())
        }
        SetEncodingMode::SparseIds => decode_sparse_ids(payload),
        SetEncodingMode::Bitmap => {
            if end < start {
                return Some(Vec::new());
            }
            let bit_len = end.saturating_sub(start).saturating_add(1) as usize;
            decode_bitmap_with_base(payload, start, bit_len)
        }
    }
}

pub(crate) fn encode_sparse_ids(ids: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    bytes
}

pub(crate) fn decode_sparse_ids(payload: &[u8]) -> Option<Vec<u64>> {
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

pub(crate) fn encode_bitmap_with_base(base: u64, bit_len: usize, ids: &[u64]) -> Vec<u8> {
    if bit_len == 0 {
        return Vec::new();
    }
    let mut bitmap = vec![0_u8; bit_len.div_ceil(8)];
    for id in ids {
        if *id < base {
            continue;
        }
        let index = (*id - base) as usize;
        if index >= bit_len {
            continue;
        }
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        bitmap[byte_idx] |= 1_u8 << bit_idx;
    }
    bitmap
}

pub(crate) fn decode_bitmap_with_base(
    payload: &[u8],
    base: u64,
    bit_len: usize,
) -> Option<Vec<u64>> {
    let expected_bytes = bit_len.div_ceil(8);
    if payload.len() != expected_bytes {
        return None;
    }

    let mut out = Vec::new();
    for i in 0..bit_len {
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        if payload[byte_idx] & (1_u8 << bit_idx) != 0 {
            out.push(base + i as u64);
        }
    }
    Some(out)
}

pub(crate) fn bitmap_contains_with_base(bitmap: &[u8], id: u64, base: u64) -> bool {
    if id < base {
        return false;
    }
    let bit_index = usize::try_from(id - base).unwrap_or(usize::MAX);
    let byte_index = bit_index / 8;
    if byte_index >= bitmap.len() {
        return false;
    }
    let mask = 1u8 << (bit_index % 8);
    bitmap[byte_index] & mask != 0
}
