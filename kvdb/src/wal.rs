use crate::crc::crc32;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

pub const TOMBSTONE: u32 = u32::MAX;
pub type Record = (Vec<u8>, Option<Vec<u8>>);

/// Write-ahead log. Record: crc32 | klen | vlen (u32::MAX = delete) | key | value.
/// The CRC covers everything after itself, so a torn tail write is detected on replay.
pub struct Wal {
    file: BufWriter<File>,
    sync: bool,
}

impl Wal {
    /// Opens the log, replays every valid record and truncates any torn tail.
    pub fn open(path: &Path, sync: bool) -> io::Result<(Wal, Vec<Record>)> {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        let (records, good) = Self::parse(&data);
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        if (good as u64) < data.len() as u64 {
            file.set_len(good as u64)?;
        }
        Ok((Wal { file: BufWriter::new(file), sync }, records))
    }

    fn parse(buf: &[u8]) -> (Vec<Record>, usize) {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos + 12 <= buf.len() {
            let crc = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
            let kl = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap()) as usize;
            let vl_raw = u32::from_le_bytes(buf[pos + 8..pos + 12].try_into().unwrap());
            let vl = if vl_raw == TOMBSTONE { 0 } else { vl_raw as usize };
            let total = match 12usize.checked_add(kl).and_then(|x| x.checked_add(vl)) {
                Some(t) if pos + t <= buf.len() => t,
                _ => break,
            };
            if crc32(&buf[pos + 4..pos + total]) != crc {
                break;
            }
            let key = buf[pos + 12..pos + 12 + kl].to_vec();
            let val = (vl_raw != TOMBSTONE).then(|| buf[pos + 12 + kl..pos + total].to_vec());
            out.push((key, val));
            pos += total;
        }
        (out, pos)
    }

    pub fn append(&mut self, key: &[u8], val: Option<&[u8]>) -> io::Result<()> {
        let mut body = Vec::with_capacity(8 + key.len() + val.map_or(0, |v| v.len()));
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(&val.map_or(TOMBSTONE, |v| v.len() as u32).to_le_bytes());
        body.extend_from_slice(key);
        if let Some(v) = val {
            body.extend_from_slice(v);
        }
        self.file.write_all(&crc32(&body).to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.flush()?;
        if self.sync {
            self.file.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// Empties the log (called after the memtable has been safely flushed to an SSTable).
    pub fn reset(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().set_len(0)?;
        self.file.get_ref().sync_all()
    }
}
