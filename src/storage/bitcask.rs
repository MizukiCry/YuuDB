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

#[cfg(test)]
mod tests {
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
                writeln!(
                    writer,
                    "value_length = {value_length_raw} {:x?}",
                    length_buffer
                )?;

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

    /// Creates a new BitCask engine for testing.
    fn setup() -> Result<BitCask> {
        BitCask::new(tempdir::TempDir::new("yuudb")?.path().join("yuudb"))
    }

    /// Writes various values primarily for testing log file handling.
    ///
    /// - '': empty key and value
    /// - a: write
    /// - b: write, write
    /// - c: write, delete, write
    /// - d: delete, write
    /// - e: write, delete
    /// - f: delete
    fn setup_log(s: &mut BitCask) -> Result<()> {
        s.set(b"b", vec![0x01])?;
        s.set(b"b", vec![0x02])?;

        s.set(b"e", vec![0x05])?;
        s.delete(b"e")?;

        s.set(b"c", vec![0x00])?;
        s.delete(b"c")?;
        s.set(b"c", vec![0x03])?;

        s.set(b"", vec![])?;

        s.set(b"a", vec![0x01])?;

        s.delete(b"f")?;

        s.delete(b"d")?;
        s.set(b"d", vec![0x04])?;

        // Make sure the scan yields the expected results.
        assert_eq!(
            vec![
                (b"".to_vec(), vec![]),
                (b"a".to_vec(), vec![0x01]),
                (b"b".to_vec(), vec![0x02]),
                (b"c".to_vec(), vec![0x03]),
                (b"d".to_vec(), vec![0x04]),
            ],
            s.scan(..).collect::<Result<Vec<_>>>()?,
        );

        Ok(())
    }

    #[test]
    /// Tests that logs are written correctly using a golden file.
    fn log() -> Result<()> {
        let mut s = setup()?;
        setup_log(&mut s)?;

        let mut mint = goldenfile::Mint::new(GOLDEN_DIR);
        s.log.print(&mut mint.new_goldenfile("log")?)?;
        Ok(())
    }

    #[test]
    /// Tests that writing and then reading a file yields the same results.
    fn reopen() -> Result<()> {
        // NB: Don't use setup(), because the tempdir will be removed when
        // the path falls out of scope.
        let path = tempdir::TempDir::new("yuudb")?.path().join("yuudb");
        let mut s = BitCask::new(path.clone())?;
        setup_log(&mut s)?;

        let expect = s.scan(..).collect::<Result<Vec<_>>>()?;
        drop(s);
        let mut s = BitCask::new(path)?;
        assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?,);

        Ok(())
    }

    #[test]
    /// Tests log compaction, by writing golden files of the before/after state,
    /// and checking that the database contains the same results, even after
    /// reopening the file.
    fn compact() -> Result<()> {
        // NB: Don't use setup(), because the tempdir will be removed when
        // the path falls out of scope.
        let path = tempdir::TempDir::new("yuudb")?.path().join("yuudb");
        let mut s = BitCask::new(path.clone())?;
        setup_log(&mut s)?;

        // Dump the initial log file.
        let mut mint = goldenfile::Mint::new(GOLDEN_DIR);
        s.log.print(&mut mint.new_goldenfile("compact-before")?)?;
        let expect = s.scan(..).collect::<Result<Vec<_>>>()?;

        // Compact the log file and assert the new log file contents.
        s.compact()?;
        assert_eq!(path, s.log.path);
        assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?,);
        s.log.print(&mut mint.new_goldenfile("compact-after")?)?;

        // Reopen the log file and assert that the contents are the same.
        drop(s);
        let mut s = BitCask::new(path)?;
        assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?,);

        Ok(())
    }

    #[test]
    /// Tests that new_compact() will automatically compact the file when appropriate.
    fn new_compact() -> Result<()> {
        // Create an initial log file with a few entries.
        let dir = tempdir::TempDir::new("yuudb")?;
        let path = dir.path().join("orig");
        let compactpath = dir.path().join("compact");

        let mut s = BitCask::new_compact(path.clone(), 0.2)?;
        setup_log(&mut s)?;
        let status = s.status()?;
        let garbage_ratio = status.garbage_disk_size as f64 / status.total_disk_size as f64;
        drop(s);

        // Test a few threshold value and assert whether it should trigger compaction.
        let cases = vec![
            (-1.0, true),
            (0.0, true),
            (garbage_ratio - 0.001, true),
            (garbage_ratio, true),
            (garbage_ratio + 0.001, false),
            (1.0, false),
            (2.0, false),
        ];
        for (threshold, expect_compact) in cases.into_iter() {
            std::fs::copy(&path, &compactpath)?;
            let mut s = BitCask::new_compact(compactpath.clone(), threshold)?;
            let new_status = s.status()?;
            assert_eq!(new_status.live_disk_size, status.live_disk_size);
            if expect_compact {
                assert_eq!(new_status.total_disk_size, status.live_disk_size);
                assert_eq!(new_status.garbage_disk_size, 0);
            } else {
                assert_eq!(new_status, status);
            }
        }

        Ok(())
    }

    #[test]
    /// Tests that exclusive locks are taken out on log files, released when the
    /// database is closed, and that an error is returned if a lock is already
    /// held.
    fn log_lock() -> Result<()> {
        let path = tempdir::TempDir::new("yuudb")?.path().join("yuudb");
        let s = BitCask::new(path.clone())?;

        assert!(BitCask::new(path.clone()).is_err());
        drop(s);
        assert!(BitCask::new(path.clone()).is_ok());

        Ok(())
    }

    #[test]
    /// Tests that an incomplete write at the end of the log file can be
    /// recovered by discarding the last entry.
    fn recovery() -> Result<()> {
        // Create an initial log file with a few entries.
        let dir = tempdir::TempDir::new("yuudb")?;
        let path = dir.path().join("complete");
        let truncpath = dir.path().join("truncated");

        let mut log = Log::new(path.clone())?;
        let mut ends = vec![];

        let (pos, len) = log.append_entry("deleted".as_bytes(), Some(&[1, 2, 3]))?;
        ends.push(pos + len as u64);

        let (pos, len) = log.append_entry("deleted".as_bytes(), None)?;
        ends.push(pos + len as u64);

        let (pos, len) = log.append_entry(&[], Some(&[]))?;
        ends.push(pos + len as u64);

        let (pos, len) = log.append_entry("key".as_bytes(), Some(&[1, 2, 3, 4, 5]))?;
        ends.push(pos + len as u64);

        drop(log);

        // Copy the file, and truncate it at each byte, then try to open it
        // and assert that we always retain a prefix of entries.
        let size = std::fs::metadata(&path)?.len();
        for pos in 0..=size {
            std::fs::copy(&path, &truncpath)?;
            let f = std::fs::OpenOptions::new().write(true).open(&truncpath)?;
            f.set_len(pos)?;
            drop(f);

            let mut expect = vec![];
            if pos >= ends[0] {
                expect.push((b"deleted".to_vec(), vec![1, 2, 3]))
            }
            if pos >= ends[1] {
                expect.pop(); // "deleted" key removed
            }
            if pos >= ends[2] {
                expect.push((b"".to_vec(), vec![]))
            }
            if pos >= ends[3] {
                expect.push((b"key".to_vec(), vec![1, 2, 3, 4, 5]))
            }

            let mut s = BitCask::new(truncpath.clone())?;
            assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?);
        }

        Ok(())
    }

    #[test]
    /// Tests status(), both for a log file with known garbage, and
    /// after compacting it when the live size must equal the file size.
    fn status_full() -> Result<()> {
        let mut s = setup()?;
        setup_log(&mut s)?;

        // Before compaction.
        assert_eq!(
            s.status()?,
            Status {
                name: "bitcask".to_string(),
                key_count: 5,
                size: 8,
                total_disk_size: 114,
                live_disk_size: 48,
                garbage_disk_size: 66
            }
        );

        // After compaction.
        s.compact()?;
        assert_eq!(
            s.status()?,
            Status {
                name: "bitcask".to_string(),
                key_count: 5,
                size: 8,
                total_disk_size: 48,
                live_disk_size: 48,
                garbage_disk_size: 0,
            }
        );

        Ok(())
    }
}
