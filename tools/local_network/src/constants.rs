pub(crate) const BLAZEGRAPH_URL: &str = "http://localhost:9999";

pub(crate) const DEFAULT_NODES: usize = 12;
pub(crate) const DEFAULT_JS_NODES: usize = 0;
pub(crate) const HTTP_PORT_BASE: usize = 8900;
pub(crate) const NETWORK_PORT_BASE: usize = 9100;
pub(crate) const HARDHAT_PORT: u16 = 8545;
pub(crate) const HARDHAT_BLOCKCHAIN_ID: &str = "hardhat1:31337";
pub(crate) const BOOTSTRAP_NODE_INDEX: usize = 0;
pub(crate) const BOOTSTRAP_KEY_PATH: &str = "network/private_key";
pub(crate) const BINARY_NAME: &str = "rust-dkg-engine";

pub(crate) const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
pub(crate) const PUBLIC_KEYS_PATH: &str = "./tools/local_network/src/public_keys.json";
pub(crate) const MANAGEMENT_PRIVATE_KEYS_PATH: &str =
    "./dkg-engine/test/bdd/steps/api/datasets/privateKeys-management-wallets.json";
pub(crate) const MANAGEMENT_PUBLIC_KEYS_PATH: &str =
    "./dkg-engine/test/bdd/steps/api/datasets/publicKeys-management-wallets.json";

// Known bootstrap private key that generates peer ID:
// QmWyf3dtqJnhuCpzEDTNmNFYc5tjxTrXhGcUUmGHdg2gtj
pub(crate) const JS_BOOTSTRAP_PRIVATE_KEY: &str = "CAAS4QQwggJdAgEAAoGBALOYSCZsmINMpFdH8ydA9CL46fB08F3ELfb9qiIq+z4RhsFwi7lByysRnYT/NLm8jZ4RvlsSqOn2ZORJwBywYD5MCvU1TbEWGKxl5LriW85ZGepUwiTZJgZdDmoLIawkpSdmUOc1Fbnflhmj/XzAxlnl30yaa/YvKgnWtZI1/IwfAgMBAAECgYEAiZq2PWqbeI6ypIVmUr87z8f0Rt7yhIWZylMVllRkaGw5WeGHzQwSRQ+cJ5j6pw1HXMOvnEwxzAGT0C6J2fFx60C6R90TPos9W0zSU+XXLHA7AtazjlSnp6vHD+RxcoUhm1RUPeKU6OuUNcQVJu1ZOx6cAcP/I8cqL38JUOOS7XECQQDex9WUKtDnpHEHU/fl7SvCt0y2FbGgGdhq6k8nrWtBladP5SoRUFuQhCY8a20fszyiAIfxQrtpQw1iFPBpzoq1AkEAzl/s3XPGi5vFSNGLsLqbVKbvoW9RUaGN8o4rU9oZmPFL31Jo9FLA744YRer6dYE7jJMel7h9VVWsqa9oLGS8AwJALYwfv45Nbb6yGTRyr4Cg/MtrFKM00K3YEGvdSRhsoFkPfwc0ZZvPTKmoA5xXEC8eC2UeZhYlqOy7lL0BNjCzLQJBAMpvcgtwa8u6SvU5B0ueYIvTDLBQX3YxgOny5zFjeUR7PS+cyPMQ0cyql8jNzEzDLcSg85tkDx1L4wi31Pnm/j0CQFH/6MYn3r9benPm2bYSe9aoJp7y6ht2DmXmoveNbjlEbb8f7jAvYoTklJxmJCcrdbNx/iCj2BuAinPPgEmUzfQ=";
pub(crate) const JS_BOOTSTRAP_PEER_ID: &str = "QmWyf3dtqJnhuCpzEDTNmNFYc5tjxTrXhGcUUmGHdg2gtj";
