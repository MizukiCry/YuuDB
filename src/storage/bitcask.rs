/*!
A variant of bitcask.
One file only, no hint files, no checksums or timestamps, locks the database while compacting.

Log entry format:
- Key length: big-endian u32
- Value length: big-endian i32, -1 for tombstones
- Key: raw bytes
- Value raw bytes

Bitcask is a fast log-structured key/value engine.
Original paper: https://riak.com/assets/bitcask-intro.pdf
*/

use super::engine::{Engine, Status};
use crate::error::Result;

use fs4::FileExt;
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

struct Log {
    path: PathBuf,
    file: std::fs::File,
}

type KeyDir = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;

impl Log {
    fn new(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.try_lock_exclusive()?;
        Ok(Self { path, file })
    }

    fn build_key_dir(&mut self) -> Result<KeyDir> {
        let mut length_buffer = [0u8; 4];
        let mut key_dir = KeyDir::new();
        let file_length = self.file.metadata()?.len();
        let mut reader = std::io::BufReader::new(&mut self.file);
        let mut offset = reader.seek(SeekFrom::Start(0))?;

        while offset < file_length {
            let result = || -> std::result::Result<(Vec<u8>, u64, Option<u32>), std::io::Error> {
                reader.read_exact(&mut length_buffer)?;
                let key_length = u32::from_be_bytes(length_buffer);

                reader.read_exact(&mut length_buffer)?;
                let value_length = match i32::from_be_bytes(length_buffer) {
                    length if !length.is_negative() => Some(length as u32),
                    _ => None,
                };
                let value_offset = offset + 4 + 4 + key_length as u64;

                let mut key = vec![0u8; key_length as usize];
                reader.read_exact(&mut key)?;

                if let Some(value_length) = value_length {
                    if value_offset + value_length as u64 > file_length {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "Value length exceeds file length",
                        ));
                    }

                    reader.seek_relative(value_length as i64)?;
                }

                Ok((key, value_offset, value_length))
            }();

            match result {
                Ok((key, value_offset, Some(value_length))) => {
                    key_dir.insert(key, (value_offset, value_length));
                    offset = value_offset + value_length as u64;
                }
                Ok((key, value_offset, None)) => {
                    key_dir.remove(&key);
                    offset = value_offset;
                }
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    log::error!("Found incomplete entry at offset {offset}, truncating file");
                    self.file.set_len(offset)?;
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Ok(key_dir)
    }

    fn read_value(&mut self, value_offset: u64, value_length: u32) -> Result<Vec<u8>> {
        let mut value = vec![0u8; value_length as usize];
        self.file.seek(SeekFrom::Start(value_offset))?;
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    fn append_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_length = key.len() as u32;
        let append_length = 4 + 4 + key_length + value.map_or(0, |v| v.len() as u32);

        let mut writer = std::io::BufWriter::with_capacity(append_length as usize, &mut self.file);
        writer.write_all(&key_length.to_be_bytes())?;
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;
        writer.write_all(key)?;
        if let Some(value) = value {
            writer.write_all(value)?;
        }
        writer.flush()?;
        Ok((offset, append_length))
    }
}

pub struct ScanIterator<'a> {
    inner: std::collections::btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> ScanIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (value_offset, value_length)) = item;
        Ok((
            key.clone(),
            self.log.read_value(*value_offset, *value_length)?,
        ))
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}

pub struct BitCask {
    log: Log,
    key_dir: KeyDir,
}

impl BitCask {
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut log = Log::new(path)?;
        let key_dir = log.build_key_dir()?;
        Ok(Self { log, key_dir })
    }

    pub fn new_compact(path: PathBuf, garbage_ratio_threshold: f64) -> Result<Self> {
        let mut bit_cask = Self::new(path)?;
        let status = bit_cask.status()?;
        let garbage_ratio = status.garbage_disk_size as f64 / status.total_disk_size as f64;
        if status.garbage_disk_size > 0 && garbage_ratio >= garbage_ratio_threshold {
            log::info!(
                "Compacting {} to remove {:.3}MB garbage ({:.0}% of {:.3}MB)",
                bit_cask.log.path.display(),
                status.garbage_disk_size / 1048576,
                garbage_ratio * 100.0,
                status.total_disk_size / 1048576,
            );
            bit_cask.compact()?;
            log::info!("Compacted");
        }
        Ok(bit_cask)
    }

    pub fn compact(&mut self) -> Result<()> {
        let mut new_path = self.log.path.clone();
        new_path.set_extension("new");
        let (mut new_log, new_key_dir) = self.write_log(new_path)?;
        std::fs::rename(&new_log.path, &self.log.path)?;
        new_log.path = self.log.path.clone();
        self.log = new_log;
        self.key_dir = new_key_dir;
        Ok(())
    }

    fn write_log(&mut self, path: PathBuf) -> Result<(Log, KeyDir)> {
        let mut new_log = Log::new(path)?;
        let mut new_key_dir = KeyDir::new();

        new_log.file.set_len(0)?;
        for (key, (value_offset, value_length)) in &self.key_dir {
            let value = self.log.read_value(*value_offset, *value_length)?;
            let (offset, write_length) = new_log.append_entry(key, Some(&value))?;
            new_key_dir.insert(
                key.clone(),
                (
                    offset + write_length as u64 - *value_length as u64,
                    *value_length,
                ),
            );
        }

        Ok((new_log, new_key_dir))
    }
}

impl std::fmt::Display for BitCask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bitcask")
    }
}

impl Engine for BitCask {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let (offset, write_length) = self.log.append_entry(key, Some(&value))?;
        let value_length = value.len() as u32;
        self.key_dir.insert(
            key.to_vec(),
            (
                offset + write_length as u64 - value_length as u64,
                value_length,
            ),
        );
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((offset, length)) = self.key_dir.get(key) {
            Ok(Some(self.log.read_value(*offset, *length)?))
        } else {
            Ok(None)
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.log.append_entry(key, None)?;
        self.key_dir.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.log.file.sync_all()?)
    }

    fn status(&mut self) -> Result<Status> {
        let name = self.to_string();
        let key_count = self.key_dir.len() as u64;
        let size = self
            .key_dir
            .iter()
            .fold(0, |size, (key, (_, value_length))| {
                size + key.len() as u64 + *value_length as u64
            });
        let total_disk_size = self.log.file.metadata()?.len();
        let live_disk_size = size + 8 * key_count;
        let garbage_disk_size = total_disk_size - live_disk_size;
        Ok(Status {
            name,
            key_count,
            size,
            total_disk_size,
            live_disk_size,
            garbage_disk_size,
        })
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_> {
        ScanIterator {
            inner: self.key_dir.range(range),
            log: &mut self.log,
        }
    }
}

impl Drop for BitCask {
    fn drop(&mut self) {
        if let Err(error) = self.flush() {
            log::error!("Failed to flush database: {}", error);
        }
    }
}

// Original tests from toyDB
#[cfg(test)]
mod tests {
    use crate::test_engine;

    use super::*;

    impl Log {
        fn print(&mut self, writer: &mut impl Write) -> Result<()> {
            let mut length_buffer = [0u8; 4];
            let file_length = self.file.metadata()?.len();
            let mut reader = std::io::BufReader::new(&mut self.file);
            let mut offset = reader.seek(SeekFrom::Start(0))?;
            let mut index = 0;

            while offset < file_length {
                writeln!(writer, "index = {index}, offset = {offset}")?;

                reader.read_exact(&mut length_buffer)?;
                let key_length = u32::from_be_bytes(length_buffer);
                writeln!(writer, "key_length = {key_length} {:x?}", length_buffer)?;

                reader.read_exact(&mut length_buffer)?;
                let value_length_raw = i32::from_be_bytes(length_buffer);
                let value_length = value_length_raw.max(0) as u32;
                writeln!(writer, "value_length = {value_length} {:x?}", length_buffer)?;

                let mut key = vec![0u8; key_length as usize];
                reader.read_exact(&mut key)?;
                write!(writer, "key = ")?;
                if let Ok(key) = std::str::from_utf8(&key) {
                    write!(writer, r#""{}" "#, key)?;
                }
                writeln!(writer, "{:x?}", key)?;

                let mut value = vec![0u8; value_length as usize];
                reader.read_exact(&mut value)?;
                write!(writer, "value = ")?;
                if value_length_raw.is_negative() {
                    write!(writer, "tombstone ")?;
                } else if let Ok(value) = std::str::from_utf8(&value) {
                    if value.chars().all(|c| !c.is_control()) {
                        write!(writer, r#""{}" "#, value)?;
                    }
                }
                writeln!(writer, "{:x?}\n", value)?;

                offset += 4 + 4 + key_length as u64 + value_length as u64;
                index += 1;
            }
            Ok(())
        }
    }

    const GOLDEN_DIR: &str = "tests/golden/bitcask";

    test_engine!(BitCast::new(
        tempdir::TempDir::new("yuudb")?.path().join("yuudb")
    )?);
}
