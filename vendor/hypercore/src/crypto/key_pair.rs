//! Generate an `Ed25519` keypair.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;

use crate::HypercoreError;

/// Key pair where for read-only hypercores the secret key can also be missing.
#[derive(Debug, Clone)]
pub struct PartialKeypair {
    /// Public key
    pub public: VerifyingKey,
    /// Secret key. If None, the hypercore is read-only.
    pub secret: Option<SigningKey>,
}

/// Generate a new `Ed25519` key pair.
pub fn generate() -> SigningKey {
    let mut csprng = OsRng;
    SigningKey::generate(&mut csprng)
}

/// Sign a byte slice using a keypair's private key.
pub fn sign(signing_key: &SigningKey, msg: &[u8]) -> Signature {
    signing_key.sign(msg)
}

/// Verify a signature on a message with a keypair's public key.
pub fn verify(
    public: &VerifyingKey,
    msg: &[u8],
    sig: Option<&Signature>,
) -> Result<(), HypercoreError> {
    match sig {
        None => Err(HypercoreError::InvalidSignature {
            context: "No signature provided.".to_string(),
        }),
        Some(sig) => {
            if public.verify(msg, sig).is_ok() {
                Ok(())
            } else {
                Err(HypercoreError::InvalidSignature {
                    context: "Signature could not be verified.".to_string(),
                })
            }
        }
    }
}

#[test]
fn can_verify_messages() {
    let signing_key = generate();
    let from = b"hello";
    let sig = sign(&signing_key, from);
    verify(&signing_key.verifying_key(), from, Some(&sig)).unwrap();
    verify(&signing_key.verifying_key(), b"oops", Some(&sig)).unwrap_err();
}
