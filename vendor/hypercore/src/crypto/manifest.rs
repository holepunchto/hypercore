// These the output of the following link:
// https://github.com/holepunchto/hypercore/blob/cf08b72f14ed7d9ef6d497ebb3071ee0ae20967e/lib/caps.js#L16

const DEFAULT_NAMESPACE: [u8; 32] = [
    0x41, 0x44, 0xEE, 0xA5, 0x31, 0xE4, 0x83, 0xD5, 0x4E, 0x0C, 0x14, 0xF4, 0xCA, 0x68, 0xE0, 0x64,
    0x4F, 0x35, 0x53, 0x43, 0xFF, 0x6F, 0xCB, 0x0F, 0x00, 0x52, 0x00, 0xE1, 0x2C, 0xD7, 0x47, 0xCB,
];

// TODO: Eventually this would be used in manifestHash
// https://github.com/holepunchto/hypercore/blob/cf08b72f14ed7d9ef6d497ebb3071ee0ae20967e/lib/manifest.js#L211
//
// const MANIFEST: [u8; 32] = [
//     0xE6, 0x4B, 0x71, 0x08, 0xEA, 0xCC, 0xE4, 0x7C, 0xFC, 0x61, 0xAC, 0x85, 0x05, 0x68, 0xF5, 0x5F,
//     0x8B, 0x15, 0xB8, 0x2E, 0xC5, 0xED, 0x78, 0xC4, 0xEC, 0x59, 0x7B, 0x03, 0x6E, 0x2A, 0x14, 0x98,
// ];

#[derive(Debug, Clone)]
pub(crate) struct Manifest {
    pub(crate) hash: String,
    // TODO: In v11 can be static
    // pub(crate) static_core: Option<bool>,
    pub(crate) signer: ManifestSigner,
    // TODO: In v11 can have multiple signers
    // pub(crate) multiple_signers: Option<bool>,
}

#[derive(Debug, Clone)]
pub(crate) struct ManifestSigner {
    pub(crate) signature: String,
    pub(crate) namespace: [u8; 32],
    pub(crate) public_key: [u8; 32],
}

pub(crate) fn default_signer_manifest(public_key: [u8; 32]) -> Manifest {
    Manifest {
        hash: "blake2b".to_string(),
        signer: ManifestSigner {
            signature: "ed25519".to_string(),
            namespace: DEFAULT_NAMESPACE,
            public_key,
        },
    }
}
