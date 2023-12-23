use std::ops::Bound;

use crate::error::Result;

/// The status of a key-value store engine.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Status {
    pub name: String,
    pub key_count: u64,

    // Logical size
    pub size: u64,

    // On-disk size
    pub total_disk_size: u64,
    pub live_disk_size: u64,
    pub garbage_disk_size: u64,
}

/// A single-thread key-value store engine.
pub trait Engine: std::fmt::Display + Send + Sync {
    type ScanIterator<'a>: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a
    where
        Self: 'a;

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn flush(&mut self) -> Result<()>;

    fn status(&mut self) -> Result<Status>;

    // Iterates [start, end)
    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>;

    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_> {
        let start = Bound::Included(prefix.to_vec());
        let end = match prefix.iter().rposition(|b| *b != 0xff) {
            Some(i) => Bound::Excluded(
                prefix
                    .iter()
                    .take(i)
                    .copied()
                    .chain(std::iter::once(prefix[i] + 1))
                    .collect(),
            ),
            None => Bound::Unbounded,
        };
        self.scan((start, end))
    }
}

#[cfg(test)]
#[macro_export]
macro_rules! test_engine {
    ($setup:expr) => {};
}
