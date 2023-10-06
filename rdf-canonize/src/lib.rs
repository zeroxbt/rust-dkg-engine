mod identifier_issuer;
mod message_digest;
mod n_quad;
mod permuter;
mod urdna2015;

use n_quad::NQuads;
use urdna2015::URDNA2015;

pub async fn canonize(input: &str) -> Result<String, String> {
    let mut algo = URDNA2015::new();

    let n_quads = NQuads::parse(input)?;

    algo.main(n_quads).await
}
