//! kvdb: a small LSM-tree storage engine.
//!
//! Write path:  WAL (CRC-protected) -> memtable (BTreeMap) -> flush -> SSTable
//! Read path:   memtable -> SSTables (newest first), each guarded by a Bloom filter
//!              and a sparse index
//! Compaction:  streaming k-way merge of all SSTables, tombstones dropped,
//!              crash-safe through a `covers_from` marker stored in the table footer.
pub mod bloom;
pub mod crc;
pub mod engine;
pub mod resp;
pub mod sstable;
pub mod wal;
