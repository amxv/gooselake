use super::RuntimeSessionManager;
use crate::TurnAdmissionRecord;

impl RuntimeSessionManager {
    pub async fn admitted_turn_for_correlation(
        &self,
        session_id: &str,
        correlation_id: &str,
    ) -> Option<String> {
        self.turn_admissions
            .read()
            .await
            .values()
            .find(|admission| {
                admission.session_id == session_id
                    && admission.correlation.correlation_id.as_deref() == Some(correlation_id)
            })
            .map(|admission| admission.turn_id.clone())
    }

    pub async fn latest_turn_admission_for_correlation(
        &self,
        session_id: &str,
        correlation_id: &str,
    ) -> Option<TurnAdmissionRecord> {
        self.turn_admissions
            .read()
            .await
            .values()
            .filter(|admission| {
                admission.session_id == session_id
                    && admission.correlation.correlation_id.as_deref() == Some(correlation_id)
            })
            .max_by(|left, right| {
                left.admitted_at
                    .cmp(&right.admitted_at)
                    .then(left.updated_at.cmp(&right.updated_at))
                    .then(left.turn_id.cmp(&right.turn_id))
            })
            .cloned()
    }
}
