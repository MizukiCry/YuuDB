use crate::error::Result;

pub struct Memory {
    data: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            data: std::collections::BTreeMap::new(),
        }
    }
}

impl std::fmt::Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

pub struct ScanIterator<'a> {
    inner: std::collections::btree_map::Range<'a, Vec<u8>, Vec<u8>>,
}

impl<'a> ScanIterator<'a> {
    fn map(item: (&Vec<u8>, &Vec<u8>)) -> <Self as Iterator>::Item {
        let (key, value) = item;
        Ok((key.clone(), value.clone()))
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Self::map)
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(Self::map)
    }
}

impl super::engine::Engine for Memory {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn status(&mut self) -> Result<super::engine::Status> {
        Ok(super::engine::Status {
            name: self.to_string(),
            key_count: self.data.len() as u64,
            size: self.data.iter().fold(0, |size, (key, value)| {
                size + key.len() as u64 + value.len() as u64
            }),
            total_disk_size: 0,
            live_disk_size: 0,
            garbage_disk_size: 0,
        })
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
        ScanIterator {
            inner: self.data.range(range),
        }
    }
}
