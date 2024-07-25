#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{self, KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{self, Manifest, ManifestRecord};
use crate::mem_table::{map_bound, map_bound_with_ts, MemTable};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };

        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

/// range_overlap checks if the keys of two ranges overlap
fn range_overlap(
    range_lower: KeySlice,
    range_upper: KeySlice,
    user_lower: Bound<&[u8]>,
    user_upper: Bound<&[u8]>,
) -> bool {
    match user_lower {
        Bound::Included(lower) => {
            if lower > range_upper.key_ref() {
                return false;
            }
        }
        Bound::Excluded(lower) => {
            if lower >= range_upper.key_ref() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }

    match user_upper {
        Bound::Excluded(upper) => {
            if upper <= range_lower.key_ref() {
                return false;
            }
        }
        Bound::Included(upper) => {
            if upper < range_lower.key_ref() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }

    true
}

/// key_within checks if the key is in the range
fn key_within(key: &[u8], range_lower: KeySlice, range_upper: KeySlice) -> bool {
    return key >= range_lower.key_ref() && key <= range_upper.key_ref();
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        {
            let mut compact_thread = self.compaction_thread.lock();
            if let Some(handle) = compact_thread.take() {
                handle.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
            }
        }

        {
            let mut flush_thread = self.flush_thread.lock();
            if let Some(handle) = flush_thread.take() {
                handle.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
            }
        }

        if !self.inner.options.enable_wal {
            // 可能还有没flush的
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?;
            }

            while {
                let guard = self.inner.state.read();
                !guard.imm_memtables.is_empty()
            } {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1 << 20));
        let manifest;
        let mut memtables = BTreeSet::new();
        let mut next_sst_id = 1;
        let mut max_ts = 0;

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path).expect("create manifest file error");
            manifest.add_record_when_init(ManifestRecord::NewMemtable(0))?;
        } else {
            let (file, records) = Manifest::recover(manifest_path)?;

            for record in records {
                match record {
                    manifest::ManifestRecord::Flush(sst_id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }

                        let res = memtables.remove(&sst_id);
                        assert!(res, "{:?}", sst_id);

                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    manifest::ManifestRecord::NewMemtable(mem_id) => {
                        memtables.insert(mem_id);
                        next_sst_id = next_sst_id.max(mem_id);
                    }
                    manifest::ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;

                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            // println!("----- recover sstables -----");
            // 恢复sstable
            for sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().map(|(_, level)| level).flatten())
            {
                let sstable = SsTable::open(
                    *sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, *sst_id))?,
                )?;
                // println!("id: {:?}", sst_id);
                max_ts = max_ts.max(sstable.max_ts());
                state.sstables.insert(*sst_id, Arc::new(sstable));
            }
            // println!("----- end -----");
            next_sst_id += 1;

            // 恢复memtable
            if options.enable_wal {
                // println!("----- recover memtables -----");
                for mem_id in memtables.iter() {
                    // println!("id: {:?}", mem_id);
                    let memtable = MemTable::recover_from_wal(
                        *mem_id,
                        Self::path_of_wal_static(path, *mem_id),
                    )?;
                    if !memtable.is_empty() {
                        let ts = memtable
                            .map
                            .iter()
                            .map(|entry| entry.key().ts())
                            .max()
                            .unwrap_or_default();
                        max_ts = max_ts.max(ts);
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }

                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
                // println!("----- end ------");
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }

            file.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;

            next_sst_id += 1;
            manifest = file;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(max_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;
        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.new_txn()?;
        txn.get(key)
    }

    pub fn get_with_ts(self: &Arc<Self>, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        assert!(!key.is_empty(), "key should not be empty");
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // 在memtables中查找
        let mut mem_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mem_iters.push(Box::new(snapshot.memtable.scan(
            Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
            Bound::Included(KeySlice::from_slice(key, TS_RANGE_END)),
        )));

        for memtable in snapshot.imm_memtables.iter() {
            mem_iters.push(Box::new(memtable.scan(
                Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
                Bound::Included(KeySlice::from_slice(key, TS_RANGE_END)),
            )));
        }
        let mem_iter = MergeIterator::create(mem_iters);

        // 去Sstable中查找
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sstable_idx in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[sstable_idx].clone();
            if !key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                continue;
            }

            if let Some(bloom) = &table.bloom {
                if !bloom.may_contain(farmhash::fingerprint32(key)) {
                    continue;
                }
            }
            let iter = SsTableIterator::create_and_seek_to_key(
                table,
                KeySlice::from_slice(key, TS_RANGE_BEGIN),
            )?;

            l0_iters.push(Box::new(iter));
        }
        let l0_iter = MergeIterator::create(l0_iters);

        let mut concat_iters = Vec::new();
        for (_, level) in &snapshot.levels {
            let mut tables = Vec::new();
            for sstable_idx in level.iter() {
                let table = snapshot.sstables[sstable_idx].clone();
                if !key_within(
                    key,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    continue;
                }

                if let Some(bloom) = &table.bloom {
                    if !bloom.may_contain(farmhash::fingerprint32(key)) {
                        continue;
                    }
                }

                tables.push(table);
            }
            let iter = SstConcatIterator::create_and_seek_to_key(
                tables,
                KeySlice::from_slice(key, TS_RANGE_BEGIN),
            )?;

            concat_iters.push(Box::new(iter));
        }
        let level_iter = MergeIterator::create(concat_iters);

        let lsm_merge_iter = LsmIterator::new(
            TwoMergeIterator::create(TwoMergeIterator::create(mem_iter, l0_iter)?, level_iter)?,
            Bound::Unbounded,
            read_ts,
        )?;

        if lsm_merge_iter.is_valid()
            && lsm_merge_iter.key() == key
            && !lsm_merge_iter.value().is_empty()
        {
            return Ok(Some(Bytes::copy_from_slice(lsm_merge_iter.value())));
        }

        Ok(None)
    }

    pub fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if self.options.serializable {
            let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
            for record in batch {
                match record {
                    WriteBatchRecord::Put(key, value) => {
                        txn.put(key.as_ref(), value.as_ref());
                    }
                    WriteBatchRecord::Del(key) => {
                        txn.delete(key.as_ref());
                    }
                }
            }
            txn.commit()?;
        } else {
            self.write_batch_inner(batch)?;
        }

        Ok(())
    }

    pub fn write_batch_inner<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<u64> {
        let _ = self.mvcc().write_lock.lock();
        let ts = self.mvcc().latest_commit_ts() + 1;
        for record in batch.iter() {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key should not be empty!");
                    assert!(!value.is_empty(), "value should not be empty");

                    let approximate_size = {
                        let guard = self.state.read();
                        guard.memtable.put(KeySlice::from_slice(key, ts), value)?;
                        guard.memtable.approximate_size()
                    };

                    self.try_to_freeze_memtable(approximate_size)?;
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    let value: &[u8] = &[];
                    assert!(!key.is_empty(), "key should not be empty!");

                    let approximate_size = {
                        let guard = self.state.read();
                        guard.memtable.put(KeySlice::from_slice(key, ts), value)?;
                        guard.memtable.approximate_size()
                    };

                    self.try_to_freeze_memtable(approximate_size)?;
                }
            }
        }

        self.mvcc().update_commit_ts(ts);

        Ok(ts)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    fn try_to_freeze_memtable(&self, approximate_size: usize) -> Result<()> {
        if approximate_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();

            // 再次检查，防止出现多次freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                // ! 解除read锁
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        let old_memtable;
        {
            let mut guard = self.state.write();

            // 更新memtable
            let mut snapshot = guard.as_ref().clone();
            old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);

            // Arc类型的浅拷贝
            snapshot.imm_memtables.insert(0, old_memtable.clone());

            *guard = Arc::new(snapshot);
        }

        old_memtable.sync_wal()?;

        if let Some(manifest) = &self.manifest {
            let record = ManifestRecord::NewMemtable(memtable_id);
            manifest.add_record(state_lock_observer, record)?;
        }
        self.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let memtable_to_flush;
        let _snapshot = {
            let guard = self.state.read();
            memtable_to_flush = guard
                .imm_memtables
                .last()
                .expect("No imm_memtable exists!")
                .clone();
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let sstable_id = memtable_to_flush.id();
        let sstable = Arc::new({
            memtable_to_flush.flush(&mut sst_builder)?;
            sst_builder.build(
                sstable_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sstable_id),
            )?
        });

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            let _mem = snapshot.imm_memtables.pop().unwrap();

            snapshot.l0_sstables.insert(0, sstable_id);

            snapshot.sstables.insert(sstable_id, sstable);

            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sstable_id))?;
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&state_lock, ManifestRecord::Flush(sstable_id))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        let mvcc = self.mvcc();
        Ok(mvcc.new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.new_txn()?;
        txn.scan(lower, upper)
    }

    pub fn scan_with_ts(
        &self,
        ts: u64,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // copy
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // 获取 memtable iters
        let mut mem_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mem_iters.push(Box::new(snapshot.memtable.scan(
            map_bound_with_ts(lower, TS_RANGE_BEGIN),
            map_bound_with_ts(upper, TS_RANGE_END),
        )));

        for memtable in snapshot.imm_memtables.iter() {
            mem_iters.push(Box::new(memtable.scan(
                map_bound_with_ts(lower, TS_RANGE_BEGIN),
                map_bound_with_ts(upper, TS_RANGE_END),
            )));
        }
        let mem_iter = MergeIterator::create(mem_iters);

        // 获取 l0 iters
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for ss_idx in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[ss_idx].clone();
            if !range_overlap(
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
                lower,
                upper,
            ) {
                continue;
            }
            let iter = match lower {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    while iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
            };

            l0_iters.push(Box::new(iter));
        }
        let l0_iter = MergeIterator::create(l0_iters);

        let mut concat_iters = Vec::new();
        for (_, level) in &snapshot.levels {
            if level.len() == 0 {
                continue;
            }
            let mut sstables = Vec::with_capacity(level.len());
            for idx in level.iter() {
                let table = snapshot.sstables[idx].clone();
                if !range_overlap(
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                    lower,
                    upper,
                ) {
                    continue;
                }

                sstables.push(table);
            }
            let iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    sstables,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        sstables,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    while iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(sstables)?,
            };

            concat_iters.push(Box::new(iter));
        }
        let level_iter = MergeIterator::create(concat_iters);

        let iter = LsmIterator::new(
            TwoMergeIterator::create(TwoMergeIterator::create(mem_iter, l0_iter)?, level_iter)?,
            map_bound(upper),
            ts,
        )?;

        Ok(FusedIterator::new(iter))
    }
}
