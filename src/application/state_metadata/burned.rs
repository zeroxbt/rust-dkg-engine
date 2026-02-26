use super::encoding::{
    SetEncoding, SetEncodingMode, decode_set_for_contiguous_range, encode_set_for_contiguous_range,
};

pub(crate) type BurnedMode = SetEncodingMode;
pub(crate) type BurnedEncoding = SetEncoding;

pub(crate) fn encode_burned_ids(start: u64, end: u64, burned: &[u64]) -> BurnedEncoding {
    encode_set_for_contiguous_range(start, end, burned, 32)
}

pub(crate) fn decode_burned_ids(
    mode: BurnedMode,
    payload: &[u8],
    start: u64,
    end: u64,
) -> Option<Vec<u64>> {
    decode_set_for_contiguous_range(mode, payload, start, end)
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
