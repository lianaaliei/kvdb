use crate::sstable::{MergeIter, SsTable};
use crate::wal::Wal;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

const MAX_KEY: usize = 64 * 1024;
const MAX_VALUE: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Options {
    /// Flush the memtable to an SSTable once it reaches this many bytes.
    pub memtable_limit: usize,
    /// Run a full compaction when this many SSTables exist.
    pub compact_at: usize,
    /// fsync the WAL on every write (durable against power loss, much slower).
    pub sync: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options { memtable_limit: 4 << 20, compact_at: 4, sync: false }
    }
}

#[derive(Debug)]
pub struct Stats {
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    pub sstables: usize,
    pub sstable_entries: u64,
    pub sstable_bytes: u64,
}

struct Inner {
    mem: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    mem_bytes: usize,
    wal: Wal,
    tables: Vec<Arc<SsTable>>, // oldest -> newest
    next_id: u64,
}

pub struct Db {
    dir: PathBuf,
    opts: Options,
    inner: RwLock<Inner>,
}

fn entry_size(k: &[u8], v: &Option<Vec<u8>>) -> usize {
    k.len() + v.as_ref().map_or(0, |v| v.len()) + 32
}

fn sst_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:08}.sst"))
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> io::Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut found = Vec::new();
        for e in fs::read_dir(&dir)? {
            let p = e?.path();
            match p.extension().and_then(|s| s.to_str()) {
                Some("tmp") => {
                    let _ = fs::remove_file(&p); // unfinished flush/compaction output
                }
                Some("sst") => {
                    if let Some(id) = p.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse::<u64>().ok()) {
                        found.push((id, p));
                    }
                }
                _ => {}
            }
        }
        found.sort();
        let all = found.iter().map(|(id, p)| SsTable::open(p, *id)).collect::<io::Result<Vec<_>>>()?;

        // Finish an interrupted compaction: a table supersedes every id in [covers_from, id).
        let mut dead = HashSet::new();
        for t in &all {
            dead.extend(all.iter().filter(|o| o.id >= t.covers_from && o.id < t.id).map(|o| o.id));
        }
        let mut tables = Vec::new();
        for t in all {
            if dead.contains(&t.id) {
                fs::remove_file(&t.path)?;
            } else {
                tables.push(Arc::new(t));
            }
        }
        let next_id = tables.last().map_or(1, |t| t.id + 1);

        let (wal, records) = Wal::open(&dir.join("wal.log"), opts.sync)?;
        let mut mem = BTreeMap::new();
        let mut mem_bytes = 0;
        for (k, v) in records {
            mem_bytes += entry_size(&k, &v);
            mem.insert(k, v);
        }
        Ok(Db { dir, opts, inner: RwLock::new(Inner { mem, mem_bytes, wal, tables, next_id }) })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let inner = self.inner.read().unwrap();
        if let Some(v) = inner.mem.get(key) {
            return Ok(v.clone());
        }
        for t in inner.tables.iter().rev() {
            if let Some(v) = t.get(key)? {
                return Ok(v);
            }
        }
        Ok(None)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        if value.len() > MAX_VALUE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "value too large"));
        }
        self.write(key, Some(value))
    }

    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.write(key, None)
    }

    fn write(&self, key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        if key.is_empty() || key.len() > MAX_KEY {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid key length"));
        }
        let mut inner = self.inner.write().unwrap();
        inner.wal.append(key, value)?; // durability first
        let v = value.map(|v| v.to_vec());
        inner.mem_bytes += entry_size(key, &v);
        inner.mem.insert(key.to_vec(), v);
        if inner.mem_bytes >= self.opts.memtable_limit {
            self.flush_locked(&mut inner)?;
            if inner.tables.len() >= self.opts.compact_at {
                self.compact_locked(&mut inner)?;
            }
        }
        Ok(())
    }

    pub fn flush(&self) -> io::Result<()> {
        self.flush_locked(&mut self.inner.write().unwrap())
    }

    pub fn compact(&self) -> io::Result<()> {
        let mut inner = self.inner.write().unwrap();
        self.flush_locked(&mut inner)?;
        self.compact_locked(&mut inner)
    }

    fn flush_locked(&self, inner: &mut Inner) -> io::Result<()> {
        if inner.mem.is_empty() {
            return Ok(());
        }
        let id = inner.next_id;
        let table = SsTable::write(
            &sst_path(&self.dir, id),
            id,
            id,
            inner.mem.len(),
            inner.mem.iter().map(|(k, v)| Ok((k.clone(), v.clone()))),
        )?;
        inner.next_id += 1;
        inner.tables.push(Arc::new(table));
        inner.mem.clear();
        inner.mem_bytes = 0;
        inner.wal.reset() // safe: data is in the SSTable; replaying a stale WAL would be idempotent anyway
    }

    /// Full compaction: merge every table into one, dropping tombstones.
    /// The result replaces the NEWEST input file (atomic rename) and records which ids it
    /// supersedes, so a crash at any point leaves a state `open` can repair.
    fn compact_locked(&self, inner: &mut Inner) -> io::Result<()> {
        if inner.tables.len() < 2 {
            return Ok(());
        }
        let srcs = inner.tables.iter().map(|t| t.iter()).collect::<io::Result<Vec<_>>>()?;
        let expected = inner.tables.iter().map(|t| t.entries as usize).sum();
        let newest = inner.tables.last().unwrap().clone();
        let covers_from = inner.tables[0].covers_from;
        let merged = MergeIter::new(srcs).filter(|r| !matches!(r, Ok((_, None))));
        let out = SsTable::write(&newest.path, newest.id, covers_from, expected, merged)?;
        let old = std::mem::replace(&mut inner.tables, vec![Arc::new(out)]);
        for t in &old[..old.len() - 1] {
            let _ = fs::remove_file(&t.path);
        }
        Ok(())
    }

    pub fn stats(&self) -> Stats {
        let i = self.inner.read().unwrap();
        Stats {
            memtable_entries: i.mem.len(),
            memtable_bytes: i.mem_bytes,
            sstables: i.tables.len(),
            sstable_entries: i.tables.iter().map(|t| t.entries).sum(),
            sstable_bytes: i.tables.iter().map(|t| t.file_size).sum(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct Tmp(PathBuf);
    impl Tmp {
        fn new(name: &str) -> Tmp {
            let n = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
            let p = std::env::temp_dir().join(format!("kvdb-{name}-{}-{n}", std::process::id()));
            Tmp(p)
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn small() -> Options {
        Options { memtable_limit: 2048, compact_at: 3, sync: false }
    }

    #[test]
    fn put_get_delete() {
        let t = Tmp::new("basic");
        let db = Db::open(&t.0, Options::default()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"a", b"2").unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"2".to_vec()));
        db.delete(b"a").unwrap();
        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"missing").unwrap(), None);
        assert!(db.put(b"", b"x").is_err());
    }

    #[test]
    fn survives_restart_via_wal_and_sstables() {
        let t = Tmp::new("restart");
        {
            let db = Db::open(&t.0, small()).unwrap();
            for i in 0..500u32 {
                db.put(&i.to_be_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
            db.delete(&7u32.to_be_bytes()).unwrap();
        }
        let db = Db::open(&t.0, small()).unwrap();
        assert_eq!(db.get(&499u32.to_be_bytes()).unwrap(), Some(b"v499".to_vec()));
        assert_eq!(db.get(&0u32.to_be_bytes()).unwrap(), Some(b"v0".to_vec()));
        assert_eq!(db.get(&7u32.to_be_bytes()).unwrap(), None);
    }

    #[test]
    fn compaction_drops_tombstones_and_shrinks() {
        let t = Tmp::new("compact");
        let db = Db::open(&t.0, Options { memtable_limit: 1 << 30, compact_at: 100, sync: false }).unwrap();
        for round in 0..3 {
            for i in 0..100u32 {
                db.put(&i.to_be_bytes(), format!("r{round}").as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }
        for i in 0..50u32 {
            db.delete(&i.to_be_bytes()).unwrap();
        }
        db.compact().unwrap();
        let s = db.stats();
        assert_eq!(s.sstables, 1);
        assert_eq!(s.sstable_entries, 50);
        assert_eq!(db.get(&10u32.to_be_bytes()).unwrap(), None);
        assert_eq!(db.get(&60u32.to_be_bytes()).unwrap(), Some(b"r2".to_vec()));
    }

    #[test]
    fn interrupted_compaction_does_not_resurrect_deleted_keys() {
        let t = Tmp::new("crashcompact");
        let opts = Options { memtable_limit: 1 << 30, compact_at: 100, sync: false };
        let (old_path, saved);
        {
            let db = Db::open(&t.0, opts.clone()).unwrap();
            db.put(b"k", b"old").unwrap();
            db.flush().unwrap();
            old_path = sst_path(&t.0, 1);
            saved = fs::read(&old_path).unwrap();
            db.delete(b"k").unwrap();
            db.flush().unwrap();
            db.compact().unwrap(); // tombstone dropped, table 1 deleted
        }
        // simulate a crash that happened before table 1 was removed
        fs::write(&old_path, saved).unwrap();
        let db = Db::open(&t.0, opts).unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
        assert_eq!(db.stats().sstables, 1);
    }

    #[test]
    fn torn_wal_tail_is_discarded() {
        let t = Tmp::new("torn");
        {
            let db = Db::open(&t.0, Options::default()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
        }
        let mut f = fs::OpenOptions::new().append(true).open(t.0.join("wal.log")).unwrap();
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 9, 0, 0, 0, 5, 0]).unwrap();
        drop(f);
        {
            let db = Db::open(&t.0, Options::default()).unwrap();
            assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
            db.put(b"c", b"3").unwrap();
        }
        let db = Db::open(&t.0, Options::default()).unwrap();
        assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn model_check_against_btreemap() {
        let t = Tmp::new("model");
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut db = Db::open(&t.0, small()).unwrap();
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for step in 0..20_000u32 {
            let key = format!("key{:03}", rnd() % 300).into_bytes();
            match rnd() % 10 {
                0..=5 => {
                    let v = format!("val-{step}").into_bytes();
                    db.put(&key, &v).unwrap();
                    model.insert(key, v);
                }
                6..=7 => {
                    db.delete(&key).unwrap();
                    model.remove(&key);
                }
                _ => assert_eq!(db.get(&key).unwrap().as_ref(), model.get(&key)),
            }
            if step % 4000 == 3999 {
                drop(db);
                db = Db::open(&t.0, small()).unwrap();
            }
        }
        for i in 0..300 {
            let k = format!("key{i:03}").into_bytes();
            assert_eq!(db.get(&k).unwrap().as_ref(), model.get(&k), "key {i}");
        }
    }
}
