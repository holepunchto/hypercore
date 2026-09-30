use compact_encoding::EncodingError;
use thiserror::Error;

use crate::Store;

/// Common error type for the hypercore interface
#[derive(Error, Debug)]
pub enum HypercoreError {
    /// Bad argument
    #[error("Bad argument. {context}")]
    BadArgument {
        /// Context for the error
        context: String,
    },
    /// Not writable
    #[error("Hypercore not writable")]
    NotWritable,
    /// Invalid signature
    #[error("Given signature was invalid. {context}")]
    InvalidSignature {
        /// Context for the error
        context: String,
    },
    /// Invalid checksum
    #[error("Invalid checksum. {context}")]
    InvalidChecksum {
        /// Context for the error
        context: String,
    },
    /// Empty storage
    #[error("Empty storage: {store}.")]
    EmptyStorage {
        /// Store that was found empty
        store: Store,
    },
    /// Corrupt storage
    #[error("Corrupt storage: {store}.{}",
          .context.as_ref().map_or_else(String::new, |ctx| format!(" Context: {ctx}.")))]
    CorruptStorage {
        /// Store that was corrupt
        store: Store,
        /// Context for the error
        context: Option<String>,
    },
    /// Invalid operation
    #[error("Invalid operation. {context}")]
    InvalidOperation {
        /// Context for the error
        context: String,
    },
    /// Unexpected IO error occured
    #[error("Unrecoverable input/output error occured.{}",
          .context.as_ref().map_or_else(String::new, |ctx| format!(" {ctx}.")))]
    IO {
        /// Context for the error
        context: Option<String>,
        /// Original source error
        #[source]
        source: std::io::Error,
    },
}

impl From<std::io::Error> for HypercoreError {
    fn from(err: std::io::Error) -> Self {
        Self::IO {
            context: None,
            source: err,
        }
    }
}

impl From<EncodingError> for HypercoreError {
    fn from(err: EncodingError) -> Self {
        Self::InvalidOperation {
            context: format!("Encoding failed: {err}"),
        }
    }
}
