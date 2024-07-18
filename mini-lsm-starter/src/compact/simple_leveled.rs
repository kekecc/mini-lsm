use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut current = 0;
        let mut lower = 1;

        while lower <= self.options.max_levels {
            if current == 0 {
                let trigger =
                    _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger;

                if trigger {
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: None,
                        upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                        lower_level: lower,
                        lower_level_sst_ids: _snapshot.levels[lower - 1].1.clone(),
                        is_lower_level_bottom_level: self.options.max_levels == 1,
                    });
                }
            } else {
                if _snapshot.levels[current - 1].1.len() != 0 {
                    let size_ratio_percent = _snapshot.levels[lower - 1].1.len() as f64
                        / _snapshot.levels[current - 1].1.len() as f64;

                    if size_ratio_percent < self.options.size_ratio_percent as f64 / 100.0 {
                        return Some(SimpleLeveledCompactionTask {
                            upper_level: Some(current),
                            upper_level_sst_ids: _snapshot.levels[current - 1].1.clone(),
                            lower_level: lower,
                            lower_level_sst_ids: _snapshot.levels[lower - 1].1.clone(),
                            is_lower_level_bottom_level: lower == self.options.max_levels,
                        });
                    }
                }
            }

            current = lower;
            lower += 1;
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_rm = Vec::new();
        if let Some(upper_level) = _task.upper_level {
            assert_eq!(
                _task.upper_level_sst_ids,
                snapshot.levels[upper_level - 1].1
            );

            for idx in _task.upper_level_sst_ids.iter() {
                files_to_rm.push(*idx);
            }
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            for idx in &_task.upper_level_sst_ids {
                files_to_rm.push(*idx);
            }

            let mut set: HashSet<usize> =
                HashSet::from_iter(_task.upper_level_sst_ids.clone().into_iter());
            let new_l0_sstables: Vec<usize> = snapshot
                .l0_sstables
                .iter()
                .filter(|x| !set.remove(x))
                .copied()
                .collect();

            snapshot.l0_sstables = new_l0_sstables;
        }

        assert_eq!(
            _task.lower_level_sst_ids,
            snapshot.levels[_task.lower_level - 1].1
        );

        for idx in _task.lower_level_sst_ids.iter() {
            files_to_rm.push(*idx);
        }

        snapshot.levels[_task.lower_level - 1].1 = _output.to_vec();

        (snapshot, files_to_rm)
    }
}
