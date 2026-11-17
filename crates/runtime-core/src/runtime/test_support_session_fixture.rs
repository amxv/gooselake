//! Explicit stored-session setup for core tests. This module is cfg(test).
//! It creates no provider attachment and fabricates no native lifecycle receipt.

use super::helpers::now_ms;
use super::{CreateSessionInput, RuntimeSessionManager};
use crate::{RuntimeError, SessionRecord};

pub(crate) fn stored_session_record_for_test(
    input: CreateSessionInput,
    session_id: String,
    native_ref_prefix: &str,
    created_at: i64,
) -> Result<SessionRecord, RuntimeError> {
    if native_ref_prefix.is_empty() || native_ref_prefix.trim() != native_ref_prefix {
        return Err(RuntimeError::InvalidState(
            "test fixture native reference prefix must be nonempty and exact".into(),
        ));
    }
    Ok(SessionRecord {
        provider: input.provider.as_str().into(),
        provider_session_ref: Some(format!("{native_ref_prefix}{session_id}")),
        canonical_provider_session_ref: None,
        id: session_id,
        status: "ready".into(),
        cwd: input.cwd,
        model: input.model,
        permission_mode: input.permission_mode,
        system_prompt: None,
        metadata: input.metadata.unwrap_or_else(|| serde_json::json!({})),
        active_turn_id: None,
        worktree_id: None,
        created_at,
        updated_at: created_at,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    })
}

impl RuntimeSessionManager {
    /// Setup only: the caller seeds a fake's previously attached session before
    /// concurrent runtime work. The prefix explicitly names that fake's native
    /// identity convention; input fields are retained without normalization.
    pub(crate) async fn seed_stored_session_for_test(
        &self,
        input: CreateSessionInput,
        native_ref_prefix: &str,
    ) -> Result<SessionRecord, RuntimeError> {
        let created_at = now_ms();
        let session_id = self.allocate_id("sess", input.provider.as_str());
        let record =
            stored_session_record_for_test(input, session_id, native_ref_prefix, created_at)?;
        self.store.upsert_session(&record)?;
        self.sessions
            .write()
            .await
            .insert(record.id.clone(), record.clone());
        Ok(record)
    }
}
