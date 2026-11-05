use crate::bloom::Bloom;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::iter::Peekable;
use std::path::{Path, PathBuf};

pub type Entry = (Vec<u8>, Option<Vec<u8>>);

const MAGIC: u64 = 0x4B56_5353_5402_0001;
const FOOTER: u64 = 40;
const INDEX_EVERY: usize = 16;
const TOMB: u32 = u32::MAX;
const MAX_LEN: usize = 1 << 30;

fn bad(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn write_entry<W: Write>(w: &mut W, k: &[u8], v: &Option<Vec<u8>>) -> io::Result<u64> {
    w.write_all(&(k.len() as u32).to_le_bytes())?;
    match v {
        Some(v) => {
            w.write_all(&(v.len() as u32).to_le_bytes())?;
            w.write_all(k)?;
            w.write_all(v)?;
            Ok(8 + k.len() as u64 + v.len() as u64)
        }
        None => {
            w.write_all(&TOMB.to_le_bytes())?;
            w.write_all(k)?;
            Ok(8 + k.len() as u64)
        }
    }
}

fn read_entry<R: Read>(r: &mut R) -> io::Result<(Entry, u64)> {
    let kl = read_u32(r)? as usize;
    let vl = read_u32(r)?;
    if kl > MAX_LEN || (vl != TOMB && vl as usize > MAX_LEN) {
        return Err(bad("entry too large"));
    }
    let mut k = vec![0; kl];
    r.read_exact(&mut k)?;
    if vl == TOMB {
        return Ok(((k, None), 8 + kl as u64));
    }
    let mut v = vec![0; vl as usize];
    r.read_exact(&mut v)?;
    Ok(((k, Some(v)), 8 + kl as u64 + vl as u64))
}

/// Immutable sorted table on disk.
///
/// Layout: [entries...][sparse index][bloom filter][footer: index_off, bloom_off,
/// entry count, covers_from, magic]. Index and bloom live in memory after `open`.
pub struct SsTable {
    pub path: PathBuf,
    pub id: u64,
    /// Lowest table id this table supersedes (== id for a plain flush).
    /// Used on startup to finish an interrupted compaction.
    pub covers_from: u64,
    pub entries: u64,
    pub file_size: u64,
    index: Vec<(Vec<u8>, u64)>,
    bloom: Bloom,
    data_end: u64,
}

impl SsTable {
    pub fn write<I>(path: &Path, id: u64, covers_from: u64, expected: usize, entries: I) -> io::Result<SsTable>
    where
        I: Iterator<Item = io::Result<Entry>>,
    {
        let tmp = path.with_extension("tmp");
        if let Err(e) = Self::write_inner(&tmp, covers_from, expected, entries) {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        fs::rename(&tmp, path)?; // atomic publish
        Self::open(path, id)
    }

    fn write_inner<I>(tmp: &Path, covers_from: u64, expected: usize, entries: I) -> io::Result<()>
    where
        I: Iterator<Item = io::Result<Entry>>,
    {
        let mut w = BufWriter::new(File::create(tmp)?);
        let mut bloom = Bloom::with_capacity(expected);
        let mut index: Vec<(Vec<u8>, u64)> = Vec::new();
        let (mut off, mut n) = (0u64, 0u64);
        let mut prev: Option<Vec<u8>> = None;
        for item in entries {
            let (k, v) = item?;
            if let Some(p) = &prev {
                if *p >= k {
                    return Err(bad("keys must be strictly increasing"));
                }
            }
            if n as usize % INDEX_EVERY == 0 {
                index.push((k.clone(), off));
            }
            bloom.insert(&k);
            off += write_entry(&mut w, &k, &v)?;
            prev = Some(k);
            n += 1;
        }
        let index_off = off;
        w.write_all(&(index.len() as u32).to_le_bytes())?;
        let mut pos = index_off + 4;
        for (k, o) in &index {
            w.write_all(&(k.len() as u32).to_le_bytes())?;
            w.write_all(k)?;
            w.write_all(&o.to_le_bytes())?;
            pos += 4 + k.len() as u64 + 8;
        }
        let bloom_off = pos;
        let mut bb = Vec::new();
        bloom.encode(&mut bb);
        w.write_all(&bb)?;
        for v in [index_off, bloom_off, n, covers_from, MAGIC] {
            w.write_all(&v.to_le_bytes())?;
        }
        w.flush()?;
        w.get_ref().sync_all()
    }

    pub fn open(path: &Path, id: u64) -> io::Result<SsTable> {
        let mut f = File::open(path)?;
        let len = f.metadata()?.len();
        if len < FOOTER {
            return Err(bad("sstable too small"));
        }
        f.seek(SeekFrom::Start(len - FOOTER))?;
        let mut c = Cursor::new({
            let mut b = [0u8; FOOTER as usize];
            f.read_exact(&mut b)?;
            b
        });
        let (index_off, bloom_off, entries, covers_from, magic) =
            (read_u64(&mut c)?, read_u64(&mut c)?, read_u64(&mut c)?, read_u64(&mut c)?, read_u64(&mut c)?);
        if magic != MAGIC || index_off > bloom_off || bloom_off > len - FOOTER {
            return Err(bad("bad sstable footer"));
        }
        let mut tail = vec![0u8; (len - FOOTER - index_off) as usize];
        f.seek(SeekFrom::Start(index_off))?;
        f.read_exact(&mut tail)?;
        let split = (bloom_off - index_off) as usize;
        let mut ic = Cursor::new(&tail[..split]);
        let count = read_u32(&mut ic)? as usize;
        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            let kl = read_u32(&mut ic)? as usize;
            if kl > MAX_LEN {
                return Err(bad("bad index"));
            }
            let mut k = vec![0; kl];
            ic.read_exact(&mut k)?;
            index.push((k, read_u64(&mut ic)?));
        }
        let bloom = Bloom::decode(&tail[split..]).ok_or_else(|| bad("bad bloom"))?;
        Ok(SsTable { path: path.to_path_buf(), id, covers_from, entries, file_size: len, index, bloom, data_end: index_off })
    }

    /// `None` = key not in this table; `Some(None)` = deleted here (tombstone).
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Option<Vec<u8>>>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let slot = self.index.partition_point(|(k, _)| k.as_slice() <= key);
        if slot == 0 {
            return Ok(None);
        }
        let mut off = self.index[slot - 1].1;
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(off))?;
        let mut r = BufReader::new(f);
        for _ in 0..INDEX_EVERY {
            if off >= self.data_end {
                break;
            }
            let ((k, v), n) = read_entry(&mut r)?;
            off += n;
            match k.as_slice().cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(v)),
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }

    pub fn iter(&self) -> io::Result<SstIter> {
        Ok(SstIter { r: BufReader::new(File::open(&self.path)?), left: self.entries })
    }
}

pub struct SstIter {
    r: BufReader<File>,
    left: u64,
}

impl Iterator for SstIter {
    type Item = io::Result<Entry>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(read_entry(&mut self.r).map(|(e, _)| e))
    }
}

/// Streaming k-way merge. Sources must be ordered oldest -> newest;
/// for duplicate keys the newest source wins.
pub struct MergeIter {
    srcs: Vec<Peekable<SstIter>>,
}

impl MergeIter {
    pub fn new(srcs: Vec<SstIter>) -> Self {
        MergeIter { srcs: srcs.into_iter().map(|s| s.peekable()).collect() }
    }
}

impl Iterator for MergeIter {
    type Item = io::Result<Entry>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut min: Option<Vec<u8>> = None;
        for i in 0..self.srcs.len() {
            match self.srcs[i].peek() {
                None => {}
                Some(Err(_)) => return self.srcs[i].next(),
                Some(Ok((k, _))) => {
                    if min.as_ref().map_or(true, |m| k < m) {
                        min = Some(k.clone());
                    }
                }
            }
        }
        let min = min?;
        let mut newest = None;
        for i in 0..self.srcs.len() {
            if matches!(self.srcs[i].peek(), Some(Ok((k, _))) if *k == min) {
                if let Some(Ok((_, v))) = self.srcs[i].next() {
                    newest = Some(v); // ascending i => last write wins
                }
            }
        }
        Some(Ok((min, newest?)))
    }
}
