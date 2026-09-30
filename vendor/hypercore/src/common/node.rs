/// Node byte range
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeByteRange {
    pub(crate) index: u64,
    pub(crate) length: u64,
}
