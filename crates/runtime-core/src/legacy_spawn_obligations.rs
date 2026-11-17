//! Bounded historical spawn evidence is a fence, never native retry authority.
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyNativeTarget {
    Session(String),
    Checkout {
        worktree_id: Option<String>,
        checkout_path: Option<String>,
        repository_root: Option<String>,
        branch_name: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacySpawnScanBudget {
    pub max_rows: usize,
    pub max_bytes: usize,
}
impl Default for LegacySpawnScanBudget {
    fn default() -> Self {
        // Proposed policy values, not measured acceptance numbers.
        Self {
            max_rows: 4096,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacySpawnScanStats {
    pub source_rows_read: usize,
    pub materialized_bytes: usize,
    pub pages_read: usize,
    pub certified_receipts: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacySpawnStream {
    KeyedAdmissions,
    SpawnJournal,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyScanLimit {
    TargetBudget,
    RowBudget,
    ByteBudget,
    MetadataCellLimit,
    InvalidMetadata,
    PayloadCellLimit,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyScanIncomplete {
    pub stream: Option<LegacySpawnStream>,
    /// Last fully examined key. Diagnostic only: no cross-snapshot resume.
    pub after_operation_id: Option<String>,
    pub reason: LegacyScanLimit,
    pub stats: LegacySpawnScanStats,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacySpawnCoordinate {
    pub operation_id: String,
    pub team_id: String,
    pub stage: Option<String>,
    pub evidence_problem: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyTargetBlockReason {
    ExactSession,
    ExactWorktreeId,
    OverlappingCheckoutSpelling,
    ExactRepositoryBranch,
    UnknownSessionScope,
    UnknownCheckoutScope,
    HistoricalPathAliasUncertain,
}
#[derive(Debug, Clone)]
pub enum LegacyTargetDecision {
    Clear,
    Blocked {
        coordinate: Arc<LegacySpawnCoordinate>,
        reason: LegacyTargetBlockReason,
    },
    ScanIncomplete(Arc<LegacyScanIncomplete>),
}
#[derive(Debug, Clone)]
pub enum LegacyScanTermination {
    ExhaustedBothStreams,
    AllTargetsBlocked,
    Incomplete(Arc<LegacyScanIncomplete>),
}
#[derive(Debug, Clone)]
pub struct LegacySpawnScan {
    pub decisions: Vec<LegacyTargetDecision>,
    pub termination: LegacyScanTermination,
    pub stats: LegacySpawnScanStats,
}

/// A durable typed recovery description, independent of ID spelling and of
/// any newer canonical parent operation that encountered the legacy fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LegacyNativeRecovery {
    Blocked {
        coordinate: LegacySpawnCoordinate,
        reason: LegacyTargetBlockReason,
    },
    ScanIncomplete {
        incomplete: LegacyScanIncomplete,
    },
}

impl LegacySpawnScan {
    /// Only a complete scan can grant clearance. Known blockers retain their
    /// original coordinate; an incomplete scan never invents one.
    pub fn require_all_clear(&self) -> Result<(), crate::RuntimeError> {
        for decision in &self.decisions {
            if let LegacyTargetDecision::Blocked { coordinate, reason } = decision {
                return Err(crate::RuntimeError::LegacyNativeObligationBlocked {
                    coordinate: coordinate.as_ref().clone(),
                    reason: *reason,
                });
            }
        }
        for decision in &self.decisions {
            if let LegacyTargetDecision::ScanIncomplete(incomplete) = decision {
                return Err(crate::RuntimeError::LegacyNativeScanIncomplete {
                    incomplete: incomplete.as_ref().clone(),
                });
            }
        }
        match &self.termination {
            LegacyScanTermination::ExhaustedBothStreams => Ok(()),
            LegacyScanTermination::Incomplete(incomplete) => {
                Err(crate::RuntimeError::LegacyNativeScanIncomplete {
                    incomplete: incomplete.as_ref().clone(),
                })
            }
            LegacyScanTermination::AllTargetsBlocked => Err(crate::RuntimeError::InvalidState(
                "legacy native target scan omitted its blocked target results".into(),
            )),
        }
    }
}
