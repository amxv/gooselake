use super::*;
use crate::legacy_spawn_obligations::{
    LegacyScanIncomplete, LegacyScanLimit, LegacySpawnScanStats, LegacySpawnStream,
};

#[test]
fn inspection_paths_encode_the_original_family_independently_of_id_prefix() {
    assert_eq!(
        legacy_team_operation_inspection_path("not-prefixed/a ?#%é"),
        "/v1/diagnostics/team-operations/not-prefixed%2Fa%20%3F%23%25%C3%A9"
    );
    assert_eq!(
        canonical_operation_inspection_path("op_spawn_canonical"),
        "/v2/operations/op_spawn_canonical"
    );
    assert_eq!(
        legacy_team_operation_inspection_path(".."),
        "/v1/diagnostics/team-operations/%2E%2E"
    );
}

#[test]
fn incomplete_scan_has_no_inspection_coordinate_even_when_its_cursor_names_an_operation() {
    let recovery = LegacyNativeRecovery::ScanIncomplete {
        incomplete: LegacyScanIncomplete {
            stream: Some(LegacySpawnStream::KeyedAdmissions),
            after_operation_id: Some("last-certified-completion".into()),
            reason: LegacyScanLimit::RowBudget,
            stats: LegacySpawnScanStats::default(),
        },
    };
    let detail = recovery.error_details();
    assert_eq!(detail["kind"], "scan_incomplete");
    assert!(detail.get("inspection_path").is_none());
    assert!(detail.get("operation_id").is_none());
    assert!(detail.get("coordinate").is_none());
    assert_eq!(detail["native_retry_authorized"], false);
    let wrapped = RuntimeError::operation_unresolved_with_legacy_recovery(
        "real-canonical-owner",
        "withheld",
        Some(recovery.clone()),
    );
    assert!(
        matches!(&wrapped,RuntimeError::OperationUnresolved { operation_id,.. } if operation_id=="real-canonical-owner")
    );
    assert_eq!(wrapped.legacy_native_recovery(), Some(recovery));
}
