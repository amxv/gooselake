//! Source-derived, one-snapshot worktree walk for the bounded `gg_team status`
//! projection. An HTTP keyset page cannot be aggregated here: separate pages
//! would silently mix different SQLite snapshots during concurrent mutation.

use std::collections::HashMap;
use std::path::Path;

use runtime_core::{ManagedWorktreeStatusProjection, RuntimeError};
use rusqlite::{params_from_iter, types::Value, ToSql, Transaction, TransactionBehavior};

use crate::db::{collect_rows, db_error, open_connection};
use crate::repository_workspace_active_agent_routes::decode_active_route;
use crate::repository_worktree_status::worktree_from_row;
use crate::SqliteRuntimeRepository;

const ROOT_BATCH: usize = 200;
const WORKTREE_BATCH: usize = 100;

impl SqliteRuntimeRepository {
    /// Visit every source worktree for this real canonical root without
    /// retaining the entire catalog, all global root spellings, or the whole
    /// workspace roster. Claims and active attached routes are read from the
    /// same SQLite snapshot as their worktree, in bounded indexed batches.
    /// The visitor can perform Git eligibility checks and retain only the
    /// small output it needs. There is intentionally no cached alias map:
    /// legacy symlink roots can change identity without a database write.
    pub fn visit_managed_worktree_status_for_root(
        &self,
        canonical_repo_root: &str,
        workspace_id: &str,
        visitor: &mut dyn FnMut(
            ManagedWorktreeStatusProjection,
            Vec<String>,
        ) -> Result<(), RuntimeError>,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| {
                db_error("failed beginning streaming worktree status snapshot", error)
            })?;
        {
            let mut statement = transaction
                .prepare("SELECT DISTINCT repo_root FROM managed_worktrees ORDER BY repo_root")
                .map_err(|error| db_error("failed preparing streaming worktree roots", error))?;
            let mut rows = statement
                .query([])
                .map_err(|error| db_error("failed querying streaming worktree roots", error))?;
            let requested = Path::new(canonical_repo_root);
            let mut roots = Vec::with_capacity(ROOT_BATCH);
            while let Some(row) = rows
                .next()
                .map_err(|error| db_error("failed advancing streaming worktree roots", error))?
            {
                let root: String = row
                    .get(0)
                    .map_err(|error| db_error("failed decoding streaming worktree root", error))?;
                if std::fs::canonicalize(&root).ok().as_deref() != Some(requested) {
                    continue;
                }
                roots.push(root);
                if roots.len() == ROOT_BATCH {
                    visit_alias_batch(&transaction, &roots, workspace_id, visitor)?;
                    roots.clear();
                }
            }
            if !roots.is_empty() {
                visit_alias_batch(&transaction, &roots, workspace_id, visitor)?;
            }
        }
        transaction
            .commit()
            .map_err(|error| db_error("failed finishing streaming worktree status snapshot", error))
    }
}

fn visit_alias_batch(
    transaction: &Transaction<'_>,
    roots: &[String],
    workspace_id: &str,
    visitor: &mut dyn FnMut(
        ManagedWorktreeStatusProjection,
        Vec<String>,
    ) -> Result<(), RuntimeError>,
) -> Result<(), RuntimeError> {
    let placeholders = vec!["?"; roots.len()].join(", ");
    let mut after: Option<(String, String)> = None;
    loop {
        let mut args: Vec<&dyn ToSql> = roots.iter().map(|root| root as &dyn ToSql).collect();
        let cursor_sql = if let Some((name, id)) = after.as_ref() {
            args.push(name);
            args.push(id);
            "AND (worktree_name, id) > (?, ?)"
        } else {
            ""
        };
        let batch_limit = WORKTREE_BATCH as i64;
        args.push(&batch_limit);
        let sql = format!(
            "SELECT id, repo_root, worktree_root, worktree_cwd, branch_name,
                    worktree_name, unified_workspace_path, deletion_policy,
                    created_by_session_id, created_by_operation_id,
                    created_at, updated_at, revision
             FROM managed_worktrees WHERE repo_root IN ({placeholders})
             {cursor_sql} ORDER BY worktree_name ASC, id ASC LIMIT ?"
        );
        let selected = {
            let mut statement = transaction
                .prepare(&sql)
                .map_err(|error| db_error("failed preparing streaming worktree batch", error))?;
            let rows = statement
                .query_map(params_from_iter(args), |row| {
                    Ok((worktree_from_row(row)?, row.get::<_, i64>(12)?))
                })
                .map_err(|error| db_error("failed querying streaming worktree batch", error))?;
            collect_rows(rows)?
        };
        if selected.is_empty() {
            break;
        }
        let selected_count = selected.len();
        let last = selected.last().expect("nonempty page");
        let next_after = (last.0.worktree_name.clone(), last.0.id.clone());

        let ids = selected
            .iter()
            .map(|(worktree, _)| worktree.id.as_str())
            .collect::<Vec<_>>();
        let id_placeholders = vec!["?"; ids.len()].join(", ");
        let mut active_claims: HashMap<String, Vec<String>> = HashMap::new();
        {
            let sql = format!(
                "SELECT worktree_id, session_id FROM managed_worktree_claims
                 WHERE worktree_id IN ({id_placeholders}) AND released_at IS NULL
                 ORDER BY worktree_id ASC, session_id ASC"
            );
            let mut statement = transaction
                .prepare(&sql)
                .map_err(|error| db_error("failed preparing streaming active claims", error))?;
            let rows = statement
                .query_map(params_from_iter(ids), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| db_error("failed querying streaming active claims", error))?;
            for (id, session) in collect_rows(rows)? {
                active_claims.entry(id).or_default().push(session);
            }
        }

        let mut cwds = selected
            .iter()
            .filter(|(worktree, _)| !worktree.worktree_cwd.starts_with("__gg_tombstoned__/"))
            .map(|(worktree, _)| worktree.worktree_cwd.clone())
            .collect::<Vec<_>>();
        cwds.sort();
        cwds.dedup();
        let mut attached_by_cwd: HashMap<String, Vec<String>> = HashMap::new();
        if !cwds.is_empty() {
            let route_placeholders = vec!["?"; cwds.len()].join(", ");
            let sql = format!(
                "SELECT a.session_id, a.recreation_policy_json
                 FROM workspace_agents a
                 JOIN workspace_agent_profiles p
                   ON p.session_id=a.session_id AND p.workspace_id=a.workspace_id
                 JOIN sessions s ON s.id=a.session_id
                 WHERE a.workspace_id=? AND a.lifecycle_state='active'
                   AND json_extract(a.recreation_policy_json, '$.authoritative_cwd')
                       IN ({route_placeholders})
                 ORDER BY a.created_at ASC, a.session_id ASC"
            );
            let mut params = Vec::with_capacity(cwds.len() + 1);
            params.push(Value::Text(workspace_id.to_string()));
            params.extend(cwds.into_iter().map(Value::Text));
            let mut statement = transaction
                .prepare(&sql)
                .map_err(|error| db_error("failed preparing streaming attached routes", error))?;
            let rows = statement
                .query_map(params_from_iter(&params), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| db_error("failed querying streaming attached routes", error))?;
            for encoded in collect_rows(rows)? {
                let route = decode_active_route(encoded)?;
                attached_by_cwd
                    .entry(route.authoritative_cwd)
                    .or_default()
                    .push(route.agent_id);
            }
            for ids in attached_by_cwd.values_mut() {
                ids.sort();
            }
        }

        for (worktree, revision) in selected {
            let revision = u64::try_from(revision).map_err(|_| {
                RuntimeError::ProtocolViolation("negative managed worktree status revision".into())
            })?;
            let claims = active_claims.remove(&worktree.id).unwrap_or_default();
            let attached = attached_by_cwd
                .get(&worktree.worktree_cwd)
                .cloned()
                .unwrap_or_default();
            visitor(
                ManagedWorktreeStatusProjection {
                    worktree,
                    revision,
                    active_claim_session_ids: claims,
                },
                attached,
            )?;
        }
        if selected_count < WORKTREE_BATCH {
            break;
        }
        after = Some(next_after);
    }
    Ok(())
}
