//! Hypercore-specific compact encodings
use crate::crypto::{Manifest, ManifestSigner};
use compact_encoding::{
    CompactEncoding, EncodingError, EncodingErrorKind, encode_bytes_fixed, take_array, write_slice,
};

impl CompactEncoding for ManifestSigner {
    fn encoded_size(&self) -> Result<usize, EncodingError> {
        Ok(
            1  /* Signature */ + 32  /* namespace */ + 32, /* public_key */
        )
    }

    fn encode<'a>(&self, buffer: &'a mut [u8]) -> Result<&'a mut [u8], EncodingError> {
        let rest = if &self.signature == "ed25519" {
            write_slice(&[0], buffer)?
        } else {
            return Err(EncodingError::new(
                EncodingErrorKind::InvalidData,
                &format!("Unknown signature type: {}", &self.signature),
            ));
        };
        let rest = encode_bytes_fixed(&self.namespace, rest)?;
        encode_bytes_fixed(&self.public_key, rest)
    }

    fn decode(buffer: &[u8]) -> Result<(Self, &[u8]), EncodingError>
    where
        Self: Sized,
    {
        let ([signature_id], rest) = take_array::<1>(buffer)?;
        let signature: String = if signature_id != 0 {
            return Err(EncodingError::new(
                EncodingErrorKind::InvalidData,
                &format!("Unknown signature id: {signature_id}"),
            ));
        } else {
            "ed25519".to_string()
        };

        let (namespace, rest) = take_array::<32>(rest)?;
        let (public_key, rest) = take_array::<32>(rest)?;
        Ok((
            ManifestSigner {
                signature,
                namespace,
                public_key,
            },
            rest,
        ))
    }
}

impl CompactEncoding for Manifest {
    fn encoded_size(&self) -> Result<usize, EncodingError> {
        Ok(1 // Version
        + 1 // hash in one byte
        + 1 // type in one byte
        + self.signer.encoded_size()?)
    }

    fn encode<'a>(&self, buffer: &'a mut [u8]) -> Result<&'a mut [u8], EncodingError> {
        let rest = write_slice(&[0], buffer)?;
        let rest = if &self.hash == "blake2b" {
            write_slice(&[0], rest)?
        } else {
            return Err(EncodingError::new(
                EncodingErrorKind::InvalidData,
                &format!("Unknown hash: {}", &self.hash),
            ));
        };
        let rest = write_slice(&[1], rest)?;
        self.signer.encode(rest)
    }

    fn decode(buffer: &[u8]) -> Result<(Self, &[u8]), EncodingError>
    where
        Self: Sized,
    {
        let ([version], rest) = take_array::<1>(buffer)?;
        if version != 0 {
            panic!("Unknown manifest version {}", version);
        }
        let ([hash_id], rest) = take_array::<1>(rest)?;
        let hash: String = if hash_id != 0 {
            return Err(EncodingError::new(
                EncodingErrorKind::InvalidData,
                &format!("Unknown hash id: {hash_id}"),
            ));
        } else {
            "blake2b".to_string()
        };
        let ([manifest_type], rest) = take_array::<1>(rest)?;
        if manifest_type != 1 {
            return Err(EncodingError::new(
                EncodingErrorKind::InvalidData,
                &format!("Unknown manifest type: {manifest_type}"),
            ));
        }
        let (signer, rest) = ManifestSigner::decode(rest)?;
        Ok((Manifest { hash, signer }, rest))
    }
}
