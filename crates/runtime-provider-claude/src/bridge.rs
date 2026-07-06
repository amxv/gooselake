use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use runtime_core::{ProviderTurnResult, ProviderTurnStatus, RuntimeError};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::{mpsc, oneshot};

use crate::auth::{
    bridge_session_key, claude_smoke_debug_enabled, extract_assistant_text, extract_turn_status,
    map_bridge_error, merge_assistant_text_into_usage,
};
use crate::config::{
    CLAUDE_BRIDGE_STDIN_FLUSH_BATCH_MAX, CLAUDE_STDOUT_WORKER_LANE_COUNT,
    CLAUDE_STDOUT_WORKER_QUEUE_CAPACITY,
};
use crate::provider::{
    ClaudeBridgeEventWorkItem, ClaudeBridgeHandle, ClaudeProviderInner, OutboundJsonLine,
};

pub(crate) fn stdout_worker_lane_key_for_payload(payload: &Value) -> String {
    if let Some(bridge_session_id) = payload
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return format!("session:{bridge_session_id}");
    }

    let empty_payload = Value::Null;
    let payload_body = payload.get("payload").unwrap_or(&empty_payload);
    if let Some(turn_id) = payload
        .get("turnId")
        .or_else(|| payload_body.get("turnId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return format!("turn:{turn_id}");
    }

    let event_name = payload
        .get("event")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    format!("event:{event_name}")
}

pub(crate) fn stdout_worker_lane_index(key: &str, lane_count: usize) -> usize {
    if lane_count <= 1 {
        return 0;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % lane_count
}

pub(crate) fn spawn_stdout_worker_lanes(
    inner: Arc<ClaudeProviderInner>,
    bridge: Arc<ClaudeBridgeHandle>,
) -> Vec<mpsc::Sender<ClaudeBridgeEventWorkItem>> {
    let lane_count = CLAUDE_STDOUT_WORKER_LANE_COUNT.max(1);
    let lane_capacity = CLAUDE_STDOUT_WORKER_QUEUE_CAPACITY.max(1);
    let mut senders = Vec::with_capacity(lane_count);

    for _ in 0..lane_count {
        let (sender, mut receiver) = mpsc::channel::<ClaudeBridgeEventWorkItem>(lane_capacity);
        senders.push(sender);
        let inner = Arc::clone(&inner);
        let bridge = Arc::clone(&bridge);
        tokio::spawn(async move {
            while let Some(work_item) = receiver.recv().await {
                handle_bridge_event(&inner, &bridge, work_item.payload).await;
            }
        });
    }

    senders
}

pub(crate) fn spawn_stdout_task(
    inner: Arc<ClaudeProviderInner>,
    bridge: Arc<ClaudeBridgeHandle>,
    stdout: ChildStdout,
    worker_lane_senders: Vec<mpsc::Sender<ClaudeBridgeEventWorkItem>>,
) {
    tokio::spawn(async move {
        if worker_lane_senders.is_empty() {
            fail_bridge(
                &inner,
                &bridge,
                "Claude bridge stdout worker lanes were not initialized".to_string(),
            )
            .await;
            return;
        }

        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let payload: Value = match serde_json::from_str(line.as_str()) {
                        Ok(payload) => payload,
                        Err(_) => continue,
                    };

                    if payload.get("event").is_some() {
                        let lane_key = stdout_worker_lane_key_for_payload(&payload);
                        let lane_index =
                            stdout_worker_lane_index(lane_key.as_str(), worker_lane_senders.len());
                        if worker_lane_senders[lane_index]
                            .send(ClaudeBridgeEventWorkItem { payload })
                            .await
                            .is_err()
                        {
                            fail_bridge(
                                &inner,
                                &bridge,
                                format!(
                                    "Claude stdout worker lane {lane_index} closed unexpectedly for bridge {}",
                                    bridge.instance_id
                                ),
                            )
                            .await;
                            break;
                        }
                        continue;
                    }

                    if payload.get("id").is_some() {
                        handle_bridge_response(&bridge, payload).await;
                        continue;
                    }

                    fail_bridge(
                        &inner,
                        &bridge,
                        format!("Unexpected Claude bridge payload shape: {payload}"),
                    )
                    .await;
                    break;
                }
                Ok(None) => {
                    if !bridge.shutdown_requested.load(Ordering::SeqCst) {
                        fail_bridge(
                            &inner,
                            &bridge,
                            "Claude bridge stdout closed unexpectedly".to_string(),
                        )
                        .await;
                    }
                    break;
                }
                Err(error) => {
                    fail_bridge(
                        &inner,
                        &bridge,
                        format!("Failed reading Claude bridge stdout: {error}"),
                    )
                    .await;
                    break;
                }
            }
        }
    });
}

pub(crate) fn spawn_stderr_task(
    _inner: Arc<ClaudeProviderInner>,
    _bridge: Arc<ClaudeBridgeHandle>,
    stderr: ChildStderr,
) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    tracing::debug!("claude bridge stderr: {line}");
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });
}

pub(crate) fn spawn_stdin_writer_task(
    inner: Arc<ClaudeProviderInner>,
    bridge: Arc<ClaudeBridgeHandle>,
    stdin: ChildStdin,
    mut writer_rx: mpsc::Receiver<OutboundJsonLine>,
) {
    tokio::spawn(async move {
        let mut writer = BufWriter::new(stdin);
        loop {
            let outbound_line = tokio::select! {
                _ = bridge.writer_shutdown.notified() => break,
                outbound_line = writer_rx.recv() => outbound_line,
            };

            let Some(outbound_line) = outbound_line else {
                break;
            };

            if let Err(error) =
                write_outbound_batch(&mut writer, &mut writer_rx, outbound_line).await
            {
                fail_bridge(
                    &inner,
                    &bridge,
                    format!(
                        "Failed writing request to Claude bridge stdin for bridge {}: {error}",
                        bridge.instance_id
                    ),
                )
                .await;
                break;
            }
        }
    });
}

async fn write_outbound_batch(
    writer: &mut BufWriter<ChildStdin>,
    writer_rx: &mut mpsc::Receiver<OutboundJsonLine>,
    first_line: OutboundJsonLine,
) -> Result<(), std::io::Error> {
    writer.write_all(first_line.as_slice()).await?;
    for _ in 0..CLAUDE_BRIDGE_STDIN_FLUSH_BATCH_MAX {
        match writer_rx.try_recv() {
            Ok(next_line) => writer.write_all(next_line.as_slice()).await?,
            Err(mpsc::error::TryRecvError::Empty)
            | Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    writer.flush().await
}

pub(crate) fn spawn_heartbeat_task(
    inner: Arc<ClaudeProviderInner>,
    bridge: Arc<ClaudeBridgeHandle>,
) {
    tokio::spawn(async move {
        let mut consecutive_failures = 0_u64;
        let heartbeat_interval = Duration::from_millis(inner.config.heartbeat_interval_ms.max(1));

        loop {
            tokio::time::sleep(heartbeat_interval).await;
            if bridge.shutdown_requested.load(Ordering::SeqCst) {
                break;
            }

            let ping = send_bridge_request(
                &inner,
                &bridge,
                "bridge.ping",
                serde_json::json!({}),
                inner.config.request_timeout_ms,
            )
            .await;

            match ping {
                Ok(_) => {
                    consecutive_failures = 0;
                }
                Err(error) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= inner.config.heartbeat_failure_threshold.max(1) {
                        fail_bridge(
                            &inner,
                            &bridge,
                            format!(
                                "Claude bridge heartbeat failed {} times: {error}",
                                consecutive_failures
                            ),
                        )
                        .await;
                        break;
                    }
                }
            }
        }
    });
}

pub(crate) async fn send_bridge_request(
    inner: &Arc<ClaudeProviderInner>,
    bridge: &Arc<ClaudeBridgeHandle>,
    method: &str,
    params: Value,
    timeout_ms: u64,
) -> Result<Value, RuntimeError> {
    if bridge.closed.load(Ordering::SeqCst) {
        return Err(RuntimeError::Io(
            "Claude bridge process is not running".to_string(),
        ));
    }

    let request_id = inner
        .next_request_id
        .fetch_add(1, Ordering::SeqCst)
        .to_string();

    {
        let process = bridge.process.lock().await;
        if process.closed {
            bridge.closed.store(true, Ordering::SeqCst);
            return Err(RuntimeError::Io(
                "Claude bridge process is not running".to_string(),
            ));
        }
    }

    let (sender, receiver) = oneshot::channel();
    {
        let mut pending_requests = bridge.pending_requests.lock().await;
        pending_requests.insert(request_id.clone(), sender);
    }

    let request_payload = serde_json::json!({
        "id": request_id.clone(),
        "method": method,
        "params": params,
    });
    let mut serialized_request = serde_json::to_vec(&request_payload).map_err(|error| {
        RuntimeError::Io(format!(
            "failed serializing request to Claude bridge: {error}"
        ))
    })?;
    serialized_request.push(b'\n');

    if bridge.writer_tx.send(serialized_request).await.is_err() {
        let mut pending_requests = bridge.pending_requests.lock().await;
        pending_requests.remove(&request_id);
        drop(pending_requests);

        fail_bridge(
            inner,
            bridge,
            "Claude bridge stdin writer task closed unexpectedly".to_string(),
        )
        .await;
        return Err(RuntimeError::Io(format!(
            "failed writing request to Claude bridge for method {method}"
        )));
    }

    let response_result =
        match tokio::time::timeout(Duration::from_millis(timeout_ms.max(1)), receiver).await {
            Ok(response_result) => response_result,
            Err(_) => {
                let mut pending_requests = bridge.pending_requests.lock().await;
                pending_requests.remove(&request_id);
                return Err(RuntimeError::InvalidState(format!(
                    "timed out waiting for Claude bridge response to {method}"
                )));
            }
        };

    match response_result {
        Ok(response) => response,
        Err(_) => Err(RuntimeError::Io(format!(
            "Claude bridge response channel closed for method {method}"
        ))),
    }
}

async fn handle_bridge_response(bridge: &Arc<ClaudeBridgeHandle>, payload: Value) {
    let rpc_id = payload
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string);

    let Some(rpc_id) = rpc_id else {
        return;
    };

    let response = if let Some(result) = payload.get("result") {
        Ok(result.clone())
    } else if let Some(error) = payload.get("error") {
        Err(map_bridge_error(error))
    } else {
        Err(RuntimeError::ProtocolViolation(format!(
            "bridge response missing result/error for id {rpc_id}: {payload}"
        )))
    };

    let sender = {
        let mut pending_requests = bridge.pending_requests.lock().await;
        pending_requests.remove(&rpc_id)
    };
    if let Some(sender) = sender {
        let _ = sender.send(response);
    }
}

async fn handle_bridge_event(
    inner: &Arc<ClaudeProviderInner>,
    bridge: &Arc<ClaudeBridgeHandle>,
    payload: Value,
) {
    let event_name = payload
        .get("event")
        .and_then(Value::as_str)
        .map(str::to_string);
    let seq = payload.get("seq").and_then(Value::as_u64);
    let bridge_session_id = payload
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let payload_body = payload
        .get("payload")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let (Some(event_name), Some(seq), Some(bridge_session_id)) =
        (event_name, seq, bridge_session_id)
    else {
        fail_bridge(
            inner,
            bridge,
            format!("Bridge event missing required fields: {payload}"),
        )
        .await;
        return;
    };

    let non_monotonic_previous = {
        let mut last_event_seq_by_session = bridge.last_event_seq_by_session.lock().await;
        let previous = last_event_seq_by_session
            .get(&bridge_session_id)
            .copied()
            .unwrap_or(0);
        if seq <= previous {
            Some(previous)
        } else {
            last_event_seq_by_session.insert(bridge_session_id.clone(), seq);
            None
        }
    };
    if let Some(previous) = non_monotonic_previous {
        fail_bridge(
            inner,
            bridge,
            format!(
                "non-monotonic bridge event sequence for bridge instance {} session {}: current={seq}, previous={previous}",
                bridge.instance_id, bridge_session_id
            ),
        )
        .await;
        return;
    }

    let key = bridge_session_key(bridge.instance_id, bridge_session_id.as_str());
    let session = {
        let sessions_by_bridge_key = inner.sessions_by_bridge_key.read().await;
        sessions_by_bridge_key.get(&key).cloned()
    };
    let Some(session) = session else {
        return;
    };

    let mut event_turn_id = payload
        .get("turnId")
        .and_then(Value::as_str)
        .map(str::to_string);
    if event_turn_id.is_none() {
        event_turn_id = payload_body
            .get("turnId")
            .and_then(Value::as_str)
            .map(str::to_string);
    }
    let event_turn_id = if let Some(bridge_turn_id) = event_turn_id {
        let runtime_turn_by_bridge_turn = session.runtime_turn_by_bridge_turn.lock().await;
        Some(
            runtime_turn_by_bridge_turn
                .get(bridge_turn_id.as_str())
                .cloned()
                .unwrap_or(bridge_turn_id),
        )
    } else {
        None
    };

    match event_name.as_str() {
        _ if claude_smoke_debug_enabled() => {
            eprintln!(
                "[claude-provider] bridge event bridge_instance_id={} session_id={} seq={} event={} payload={}",
                bridge.instance_id, bridge_session_id, seq, event_name, payload_body
            );
        }
        _ => {}
    }

    match event_name.as_str() {
        "session.updated" => {
            if let Some(provider_session_ref) = payload_body
                .get("providerSessionRef")
                .and_then(Value::as_str)
                .map(str::to_string)
            {
                let mut provider_session = session.provider_session_ref.write().await;
                *provider_session = provider_session_ref;
            }
            let canonical = payload_body
                .get("claudeCanonicalSessionRef")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            if canonical.is_some() {
                let mut canonical_ref = session.canonical_provider_session_ref.write().await;
                *canonical_ref = canonical;
            }
        }
        "turn.started" => {
            if let Some(turn_id) = event_turn_id {
                let mut active_turn_id = session.active_turn_id.write().await;
                *active_turn_id = Some(turn_id);
            }
        }
        "turn.completed" => {
            if let Some(turn_id) = event_turn_id {
                let status = extract_turn_status(payload_body.get("status"));
                let assistant_text = extract_assistant_text(payload_body.get("assistant_text"))
                    .or_else(|| extract_assistant_text(payload_body.get("assistantText")));
                let turn_result = ProviderTurnResult {
                    runtime_session_id: session.runtime_session_id.clone(),
                    turn_id: turn_id.clone(),
                    status,
                    usage: merge_assistant_text_into_usage(
                        payload_body.get("usage").cloned(),
                        assistant_text,
                    ),
                    error: payload_body.get("error").cloned(),
                };
                {
                    let mut completed = session.completed_turns.lock().await;
                    completed.insert(turn_id.clone(), turn_result);
                }
                {
                    let mut active_turn_id = session.active_turn_id.write().await;
                    if active_turn_id.as_deref() == Some(turn_id.as_str()) {
                        *active_turn_id = None;
                    }
                }
                {
                    let mut bridge_turn_by_runtime_turn =
                        session.bridge_turn_by_runtime_turn.lock().await;
                    if let Some(bridge_turn_id) =
                        bridge_turn_by_runtime_turn.remove(turn_id.as_str())
                    {
                        let mut runtime_turn_by_bridge_turn =
                            session.runtime_turn_by_bridge_turn.lock().await;
                        runtime_turn_by_bridge_turn.remove(bridge_turn_id.as_str());
                    }
                }
            }
        }
        "error" => {
            if let Some(turn_id) = event_turn_id {
                let turn_result = ProviderTurnResult {
                    runtime_session_id: session.runtime_session_id.clone(),
                    turn_id: turn_id.clone(),
                    status: ProviderTurnStatus::Failed,
                    usage: None,
                    error: Some(payload_body.clone()),
                };
                {
                    let mut completed = session.completed_turns.lock().await;
                    completed.insert(turn_id.clone(), turn_result);
                }
                {
                    let mut active_turn_id = session.active_turn_id.write().await;
                    if active_turn_id.as_deref() == Some(turn_id.as_str()) {
                        *active_turn_id = None;
                    }
                }
                {
                    let mut bridge_turn_by_runtime_turn =
                        session.bridge_turn_by_runtime_turn.lock().await;
                    if let Some(bridge_turn_id) =
                        bridge_turn_by_runtime_turn.remove(turn_id.as_str())
                    {
                        let mut runtime_turn_by_bridge_turn =
                            session.runtime_turn_by_bridge_turn.lock().await;
                        runtime_turn_by_bridge_turn.remove(bridge_turn_id.as_str());
                    }
                }
            }
        }
        _ => {}
    }
}

pub(crate) async fn fail_bridge(
    inner: &Arc<ClaudeProviderInner>,
    bridge: &Arc<ClaudeBridgeHandle>,
    message: String,
) {
    let already_closed = {
        let mut process = bridge.process.lock().await;
        if process.closed {
            true
        } else {
            process.closed = true;
            bridge.closed.store(true, Ordering::SeqCst);
            bridge.shutdown_requested.store(true, Ordering::SeqCst);
            bridge.writer_shutdown.notify_waiters();
            let _ = process.child.start_kill();
            let _ = process.child.wait().await;
            false
        }
    };
    if already_closed {
        return;
    }

    let pending_requests = {
        let mut pending_requests = bridge.pending_requests.lock().await;
        std::mem::take(&mut *pending_requests)
    };

    for (_, sender) in pending_requests {
        let _ = sender.send(Err(RuntimeError::Io(message.clone())));
    }

    {
        let mut bridges = inner.bridges.write().await;
        bridges.remove(&bridge.instance_id);
    }

    let affected_sessions = {
        let sessions = inner.sessions.read().await;
        sessions
            .values()
            .filter(|session| session.bridge.instance_id == bridge.instance_id)
            .cloned()
            .collect::<Vec<_>>()
    };

    for session in affected_sessions {
        let active_turn_id = session.active_turn_id.read().await.clone();
        if let Some(turn_id) = active_turn_id {
            let mut completed = session.completed_turns.lock().await;
            completed.insert(
                turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: session.runtime_session_id.clone(),
                    turn_id,
                    status: ProviderTurnStatus::Failed,
                    usage: None,
                    error: Some(serde_json::json!({
                        "message": message,
                    })),
                },
            );
        }
    }
}
