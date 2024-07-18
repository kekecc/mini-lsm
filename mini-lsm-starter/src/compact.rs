#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};

pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };

        match _task {
            CompactionTask::Leveled(_) => unimplemented!(),
            CompactionTask::Tiered(_) => unimplemented!(),
            CompactionTask::Simple(task) => match task.upper_level {
                Some(_) => {
                    let mut first_tables = Vec::with_capacity(task.upper_level_sst_ids.len());
                    for idx in &task.upper_level_sst_ids {
                        first_tables.push(snapshot.sstables[idx].clone());
                    }
                    let first_iter = SstConcatIterator::create_and_seek_to_first(first_tables)?;

                    let mut second_tables = Vec::with_capacity(task.lower_level_sst_ids.len());
                    for idx in &task.lower_level_sst_ids {
                        second_tables.push(snapshot.sstables.get(idx).unwrap().clone());
                    }
                    let second_iter = SstConcatIterator::create_and_seek_to_first(second_tables)?;

                    let merge_iter = TwoMergeIterator::create(first_iter, second_iter)?;
                    self.compact_inner(merge_iter, _task.compact_to_bottom_level())
                }
                None => {
                    let mut l0_iters = Vec::with_capacity(task.upper_level_sst_ids.len());

                    for idx in &task.upper_level_sst_ids {
                        l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(idx).unwrap().clone(),
                        )?));
                    }
                    let first_iter = MergeIterator::create(l0_iters);

                    let mut second_tables = Vec::with_capacity(task.lower_level_sst_ids.len());
                    for idx in &task.lower_level_sst_ids {
                        second_tables.push(snapshot.sstables.get(idx).unwrap().clone());
                    }
                    let second_iter = SstConcatIterator::create_and_seek_to_first(second_tables)?;

                    let merge_iter = TwoMergeIterator::create(first_iter, second_iter)?;

                    self.compact_inner(merge_iter, _task.compact_to_bottom_level())
                }
            },
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for idx in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(idx).unwrap().clone(),
                    )?))
                }

                let mut l1_tables = Vec::with_capacity(l1_sstables.len());
                for idx in l1_sstables.iter() {
                    l1_tables.push(snapshot.sstables.get(idx).unwrap().clone());
                }

                let merge_iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_tables)?,
                )?;

                self.compact_inner(merge_iter, _task.compact_to_bottom_level())
            }
        }
    }

    fn compact_inner(
        &self,
        mut merge_iter: impl for<'a> StorageIterator<KeyType<'a> = crate::key::Key<&'a [u8]>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        // 按照大小获取SsTables
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut sstables = Vec::new();

        while merge_iter.is_valid() {
            if compact_to_bottom_level {
                if !merge_iter.value().is_empty() {
                    builder.add(merge_iter.key(), merge_iter.value());
                }
            } else {
                builder.add(merge_iter.key(), merge_iter.value());
            }

            if builder.estimated_size() >= self.options.target_sst_size {
                let old_builder = builder;
                builder = SsTableBuilder::new(self.options.block_size);

                let sst_id = self.next_sst_id();
                let sstable = old_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?;

                sstables.push(Arc::new(sstable));
            }
            merge_iter.next()?;
        }

        if builder.estimated_size() > 0 {
            let sst_id = self.next_sst_id();
            let sstable = builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;

            sstables.push(Arc::new(sstable));
        }

        Ok(sstables)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let guard = self.state.read();
            (guard.l0_sstables.clone(), guard.levels[0].1.clone())
        };

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let sstables = self.compact(&task)?;

        {
            // 修改状态
            let _guard = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();

            // 移除sstables
            for idx in state.l0_sstables.iter().chain(state.levels[0].1.iter()) {
                state.sstables.remove(idx).unwrap();
            }

            let mut l1_level = Vec::with_capacity(sstables.len());
            for sstable in sstables {
                l1_level.push(sstable.sst_id());
                let res = state.sstables.insert(sstable.sst_id(), sstable);
                assert!(res.is_none());
            }

            state.levels[0].1 = l1_level;

            let mut set: HashSet<usize> = HashSet::from_iter(l0_sstables.clone().into_iter());
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !set.remove(x))
                .copied()
                .collect();

            assert!(set.is_empty());

            *self.state.write() = Arc::new(state);
        }

        // 移除文件
        for idx in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*idx))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };

        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);

        let Some(task) = task else {
            return Ok(());
        };

        let sstables = self.compact(&task)?;
        let output: Vec<usize> = sstables.iter().map(|table| table.sst_id()).collect();
        let files_to_rm =
            {
                let state_lock = self.state_lock.lock();

                let (mut snapshot, files_to_rm) = self
                    .compaction_controller
                    .apply_compaction_result(&self.state.read(), &task, &output, true);

                for idx in files_to_rm.iter() {
                    snapshot.sstables.remove(idx);
                }

                for sstable in sstables {
                    let res = snapshot.sstables.insert(sstable.sst_id(), sstable);
                    assert!(res.is_none());
                }

                *self.state.write() = Arc::new(snapshot);

                self.sync_dir()?;
                if let Some(manifest) = &self.manifest {
                    manifest.add_record(&state_lock, ManifestRecord::Compaction(task, output))?;
                }

                files_to_rm
            };

        for file in files_to_rm {
            std::fs::remove_file(self.path_of_sst(file))?;
        }

        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let trigger;
        {
            let guard = self.state.read();
            trigger = guard.imm_memtables.len() >= self.options.num_memtable_limit;
        }

        if trigger {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
