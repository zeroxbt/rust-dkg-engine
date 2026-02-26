pub(crate) mod burned;
pub(crate) mod encoding;
pub(crate) mod private_graph;

pub(crate) use burned::{BurnedMode, decode_burned_ids, encode_burned_ids};
pub(crate) use private_graph::{
    PrivateGraphMode, PrivateGraphPresence, encode_private_graph_presence,
};
#[cfg(test)]
pub(crate) use private_graph::{decode_sparse_ids, encode_bitmap, encode_sparse_ids};
