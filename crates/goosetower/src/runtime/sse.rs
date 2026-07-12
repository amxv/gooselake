use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT};
use runtime_core::RuntimeEventRecord;
use tokio::sync::{broadcast, mpsc, watch, Notify};

use super::client::{GooselakeRuntimeClient, RuntimeClientError};
use super::events::{SourceEvent, SourceHealth, SourceHealthState};

#[derive(Debug, Clone)]
pub struct RuntimeSseFanInConfig {
    pub replay_page_limit: usize,
    pub reconnect_delay: Duration,
    pub stale_after: Duration,
}

impl Default for RuntimeSseFanInConfig {
    fn default() -> Self {
        Self {
            replay_page_limit: 2000,
            reconnect_delay: Duration::from_millis(250),
            stale_after: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseFrame {
    pub id: Option<String>,
    pub event: Option<String>,
    pub data: String,
}

#[derive(Clone)]
pub struct RuntimeSseFanIn {
    client: GooselakeRuntimeClient,
    source_epoch: String,
    config: RuntimeSseFanInConfig,
    health_tx: watch::Sender<SourceHealth>,
    epoch_change_tx: broadcast::Sender<SourceEpochChange>,
    #[cfg(test)]
    health_history: std::sync::Arc<std::sync::Mutex<Vec<SourceHealthState>>>,
}

#[derive(Debug, Clone)]
pub struct SourceEpochChange {
    pub source_epoch: String,
    pub high_watermark: i64,
    acknowledgement: std::sync::Arc<Notify>,
    installed: std::sync::Arc<std::sync::Mutex<Option<(String, i64)>>>,
}

impl SourceEpochChange {
    pub fn acknowledge(&self, source_epoch: String, high_watermark: i64) {
        *self.installed.lock().expect("epoch acknowledgement") =
            Some((source_epoch, high_watermark));
        self.acknowledgement.notify_one();
    }

    pub fn reject(&self) {
        self.acknowledgement.notify_one();
    }
}

impl RuntimeSseFanIn {
    pub fn new(
        client: GooselakeRuntimeClient,
        source_epoch: impl Into<String>,
        config: RuntimeSseFanInConfig,
    ) -> Self {
        let source_id = client.source_id().to_string();
        let source_epoch = source_epoch.into();
        let (health_tx, _) = watch::channel(SourceHealth::new(source_id, source_epoch.clone()));
        let (epoch_change_tx, _) = broadcast::channel(4);
        Self {
            client,
            source_epoch,
            config,
            health_tx,
            epoch_change_tx,
            #[cfg(test)]
            health_history: std::sync::Arc::new(std::sync::Mutex::new(vec![
                SourceHealthState::Configured,
            ])),
        }
    }

    pub fn health(&self) -> SourceHealth {
        self.health_tx.borrow().clone()
    }

    pub fn subscribe_health(&self) -> watch::Receiver<SourceHealth> {
        self.health_tx.subscribe()
    }

    pub fn subscribe_epoch_changes(&self) -> broadcast::Receiver<SourceEpochChange> {
        self.epoch_change_tx.subscribe()
    }

    #[cfg(test)]
    fn health_history(&self) -> Vec<SourceHealthState> {
        self.health_history.lock().expect("health history").clone()
    }

    pub fn spawn(
        self,
        initial_after_seq: Option<i64>,
        output: mpsc::Sender<SourceEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run_reconnecting(initial_after_seq, output).await;
        })
    }

    pub async fn run_reconnecting(
        self,
        initial_after_seq: Option<i64>,
        output: mpsc::Sender<SourceEvent>,
    ) {
        let mut cursor = initial_after_seq;
        let mut source_epoch = self.source_epoch.clone();
        loop {
            match self.client.source_bootstrap().await {
                Ok(bootstrap) if bootstrap.source_epoch != source_epoch => {
                    let acknowledgement = std::sync::Arc::new(Notify::new());
                    let change = SourceEpochChange {
                        source_epoch: bootstrap.source_epoch.clone(),
                        high_watermark: bootstrap.high_watermark,
                        acknowledgement: acknowledgement.clone(),
                        installed: std::sync::Arc::new(std::sync::Mutex::new(None)),
                    };
                    let installed = change.installed.clone();
                    let announced_cursor = Some(bootstrap.high_watermark);
                    self.transition_epoch(
                        bootstrap.source_epoch,
                        SourceHealthState::GapDetected,
                        announced_cursor,
                        Some("runtime source epoch changed; replacement snapshot required".into()),
                    );
                    if self.epoch_change_tx.send(change).is_err() {
                        self.transition(
                            SourceHealthState::Offline,
                            announced_cursor,
                            Some(
                                "runtime source epoch changed without a bootstrap consumer".into(),
                            ),
                        );
                        tokio::time::sleep(self.config.reconnect_delay).await;
                        continue;
                    }
                    acknowledgement.notified().await;
                    let Some((installed_epoch, installed_watermark)) =
                        installed.lock().expect("epoch acknowledgement").take()
                    else {
                        tokio::time::sleep(self.config.reconnect_delay).await;
                        continue;
                    };
                    source_epoch = installed_epoch;
                    cursor = Some(installed_watermark);
                }
                Ok(_) => {}
                Err(error) => {
                    self.transition(SourceHealthState::Offline, cursor, Some(error.to_string()));
                    tokio::time::sleep(self.config.reconnect_delay).await;
                    continue;
                }
            }
            self.transition(SourceHealthState::Replaying, cursor, None);
            match self
                .consume_once_with_epoch(cursor, &output, &source_epoch)
                .await
            {
                Ok(last_seq) => {
                    cursor = last_seq.or(cursor);
                    // GapDetected and stale-timeout are authoritative outcomes from consume_once.
                    // Finite EOF is the only successful exit which still reports Live here.
                    if self.health().state == SourceHealthState::Live {
                        self.transition(SourceHealthState::Stale, cursor, None);
                    }
                }
                Err(error) => {
                    cursor = self.health().last_source_seq.or(cursor);
                    self.transition(SourceHealthState::Offline, cursor, Some(error.to_string()));
                }
            }
            tokio::time::sleep(self.config.reconnect_delay).await;
        }
    }

    pub async fn consume_once(
        &self,
        after_seq: Option<i64>,
        output: &mpsc::Sender<SourceEvent>,
    ) -> Result<Option<i64>, RuntimeClientError> {
        self.consume_once_with_epoch(after_seq, output, &self.source_epoch)
            .await
    }

    async fn consume_once_with_epoch(
        &self,
        after_seq: Option<i64>,
        output: &mpsc::Sender<SourceEvent>,
        source_epoch: &str,
    ) -> Result<Option<i64>, RuntimeClientError> {
        let mut last_seq = after_seq;
        let url = self.client.endpoint(&format!(
            "/v1/events/stream?after_seq={}&limit={}",
            after_seq.unwrap_or(0),
            self.config.replay_page_limit
        ));
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        if let Some(after_seq) = after_seq {
            if let Ok(value) = HeaderValue::from_str(&after_seq.to_string()) {
                headers.insert(HeaderName::from_static("last-event-id"), value);
            }
        }

        let mut request = self.client.sse_http().get(url).headers(headers);
        if let Some(token) = self.client.bearer_token() {
            request = request.bearer_auth(token);
        }

        let response = request
            .send()
            .await
            .map_err(RuntimeClientError::Transport)?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(RuntimeClientError::Http { status, body });
        }

        self.transition(SourceHealthState::Live, last_seq, None);
        let mut parser = SseParser::default();
        let mut stream = response.bytes_stream();
        loop {
            let next = match tokio::time::timeout(self.config.stale_after, stream.next()).await {
                Ok(Some(next)) => next,
                Ok(None) => break,
                Err(_) => {
                    self.transition(SourceHealthState::Stale, last_seq, None);
                    break;
                }
            };
            let chunk = next.map_err(RuntimeClientError::Transport)?;
            self.refresh_transport_activity(source_epoch);
            for frame in parser.push(&chunk) {
                if frame.data.trim().is_empty() {
                    continue;
                }
                let runtime_event = serde_json::from_str::<RuntimeEventRecord>(&frame.data)
                    .map_err(RuntimeClientError::Json)?;
                let source_seq = runtime_event.row_id;
                if last_seq.is_some_and(|previous| source_seq <= previous) {
                    continue;
                }
                last_seq = Some(source_seq);
                let source_event = SourceEvent::from_runtime_event(
                    self.client.source_id().to_string(),
                    source_epoch.to_string(),
                    runtime_event,
                );
                self.transition(SourceHealthState::Live, Some(source_seq), None);
                if output.send(source_event).await.is_err() {
                    return Ok(last_seq);
                }
            }
        }
        Ok(last_seq)
    }

    fn refresh_transport_activity(&self, source_epoch: &str) {
        let mut health = self.health_tx.borrow().clone();
        if health.state != SourceHealthState::Live || health.source_epoch != source_epoch {
            return;
        }
        health.refresh_activity();
        self.health_tx.send_replace(health);
    }

    fn transition(
        &self,
        state: SourceHealthState,
        last_source_seq: Option<i64>,
        error: Option<String>,
    ) {
        let mut health = self.health_tx.borrow().clone();
        health.transition(state, last_source_seq, error);
        #[cfg(test)]
        self.health_history
            .lock()
            .expect("health history")
            .push(state);
        self.health_tx.send_replace(health);
    }

    fn transition_epoch(
        &self,
        source_epoch: String,
        state: SourceHealthState,
        last_source_seq: Option<i64>,
        error: Option<String>,
    ) {
        let mut health = self.health_tx.borrow().clone();
        health.source_epoch = source_epoch;
        health.last_source_seq = None;
        health.transition(state, last_source_seq, error);
        #[cfg(test)]
        self.health_history
            .lock()
            .expect("health history")
            .push(state);
        self.health_tx.send_replace(health);
    }
}

#[derive(Debug, Default)]
pub struct SseParser {
    buffer: String,
    event_id: Option<String>,
    event_name: Option<String>,
    data_lines: Vec<String>,
}

impl SseParser {
    pub fn push(&mut self, chunk: &Bytes) -> Vec<SseFrame> {
        self.buffer.push_str(&String::from_utf8_lossy(chunk));
        let mut frames = Vec::new();

        while let Some(line_end) = self.buffer.find('\n') {
            let mut line = self.buffer[..line_end].to_string();
            self.buffer.drain(..=line_end);
            if line.ends_with('\r') {
                line.pop();
            }
            if let Some(frame) = self.consume_line(&line) {
                frames.push(frame);
            }
        }

        frames
    }

    fn consume_line(&mut self, line: &str) -> Option<SseFrame> {
        if line.is_empty() {
            if self.data_lines.is_empty() {
                return None;
            }
            let frame = SseFrame {
                id: self.event_id.take(),
                event: self.event_name.take(),
                data: self.data_lines.join("\n"),
            };
            self.data_lines.clear();
            return Some(frame);
        }

        if line.starts_with(':') {
            return None;
        }

        let (field, value) = line
            .split_once(':')
            .map(|(field, value)| (field, value.strip_prefix(' ').unwrap_or(value)))
            .unwrap_or((line, ""));

        match field {
            "id" => self.event_id = Some(value.to_string()),
            "event" => self.event_name = Some(value.to_string()),
            "data" => self.data_lines.push(value.to_string()),
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::materializer::MaterializedState;
    use crate::runtime::{GooselakeRuntimeClient, GooselakeRuntimeClientConfig};
    use axum::extract::Query;
    use axum::http::{header, HeaderMap};
    use axum::response::sse::{Event, Sse};
    use axum::routing::get;
    use axum::{Json, Router};
    use runtime_core::{RuntimeEventCriticality, RuntimeEventScope};
    use serde::Deserialize;
    use serde_json::json;
    use tokio::net::TcpListener;

    #[test]
    fn parses_multiline_sse_frames() {
        let mut parser = SseParser::default();
        let frames = parser.push(&Bytes::from_static(
            b"id: 7\nevent: runtime\ndata: {\"a\":1}\ndata: {\"b\":2}\n\n",
        ));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].id.as_deref(), Some("7"));
        assert_eq!(frames[0].event.as_deref(), Some("runtime"));
        assert_eq!(frames[0].data, "{\"a\":1}\n{\"b\":2}");
    }

    #[tokio::test]
    async fn sse_reconnect_uses_last_cursor_and_dedupes_overlap() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let addr = spawn_sse_mock(observed.clone()).await;
        let client = GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
            "local",
            format!("http://{addr}"),
            Some("runtime-token".to_string()),
        ))
        .expect("client");
        let fan_in = RuntimeSseFanIn::new(client, "epoch-test", RuntimeSseFanInConfig::default());
        let (tx, mut rx) = mpsc::channel(8);
        let first = fan_in
            .consume_once(Some(1), &tx)
            .await
            .expect("first consume");
        let second = fan_in
            .consume_once(first, &tx)
            .await
            .expect("second consume");
        drop(tx);

        let mut source_seqs = Vec::new();
        while let Some(event) = rx.recv().await {
            source_seqs.push(event.source_seq);
        }

        assert_eq!(first, Some(3));
        assert_eq!(second, Some(4));
        assert_eq!(source_seqs, vec![2, 3, 4]);
        assert_eq!(
            *observed.lock().unwrap(),
            vec![(Some(1), Some(1)), (Some(3), Some(3))]
        );
        assert_eq!(fan_in.health().state, SourceHealthState::Live);
        assert_eq!(fan_in.health().last_source_seq, Some(4));
    }

    #[tokio::test]
    async fn sse_recovers_from_decode_error_and_materializes_live_session_event() {
        let calls = Arc::new(Mutex::new(0usize));
        let addr = spawn_decode_then_session_mock(calls.clone()).await;
        let client = GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
            "local",
            format!("http://{addr}"),
            None,
        ))
        .expect("client");
        let fan_in = RuntimeSseFanIn::new(client, "epoch-test", RuntimeSseFanInConfig::default());
        let (tx, mut rx) = mpsc::channel(8);
        let decode_error = fan_in.consume_once(None, &tx).await;
        assert!(matches!(decode_error, Err(RuntimeClientError::Json(_))));
        fan_in.transition(
            SourceHealthState::Offline,
            None,
            Some("decode error".to_string()),
        );
        fan_in.transition(SourceHealthState::Replaying, None, None);

        let cursor = fan_in
            .consume_once(None, &tx)
            .await
            .expect("reconnect consume");
        drop(tx);

        assert_eq!(cursor, Some(1));
        let event = rx.recv().await.expect("source event");
        let mut state = MaterializedState::new("local", "epoch-test");
        let effect = state.reduce_source_event(event);
        assert!(!effect.duplicate);
        assert_eq!(state.sessions["sess_live"].provider, "codex");
        assert_eq!(fan_in.health().state, SourceHealthState::Live);
        assert_eq!(*calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn reconnect_loop_eof_is_stale_then_replaying_without_cursor_loss() {
        let addr = spawn_reconnect_sequence_mock(vec![vec![1], vec![2]]).await;
        let fan_in = reconnecting_fan_in(addr, Duration::from_secs(1));
        let (tx, mut rx) = mpsc::channel(8);
        let task = fan_in.clone().spawn(Some(0), tx);
        tokio::time::timeout(Duration::from_secs(1), async {
            while fan_in
                .health_history()
                .iter()
                .filter(|state| **state == SourceHealthState::Replaying)
                .count()
                < 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second replay");
        task.abort();
        assert_eq!(rx.recv().await.expect("first row").source_seq, 1);
        let history = fan_in.health_history();
        assert!(history.windows(3).any(|states| states
            == [
                SourceHealthState::Live,
                SourceHealthState::Stale,
                SourceHealthState::Replaying,
            ]));
        assert_eq!(fan_in.health().last_source_seq, Some(1));
    }

    #[tokio::test]
    async fn reconnect_loop_stale_timeout_replays_without_panicking() {
        let addr = spawn_stale_stream_mock().await;
        let fan_in = reconnecting_fan_in(addr, Duration::from_millis(15));
        let (tx, _rx) = mpsc::channel(8);
        let task = fan_in.clone().spawn(Some(7), tx);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let history = fan_in.health_history();
                if history.windows(2).any(|states| {
                    states == [SourceHealthState::Stale, SourceHealthState::Replaying]
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stale reconnect");
        task.abort();
        assert_eq!(fan_in.health().last_source_seq, Some(7));
    }

    #[tokio::test]
    async fn fan_in_forwards_gap_for_the_gateway_cursor_owner_to_repair() {
        let addr = spawn_reconnect_sequence_mock(vec![vec![1, 3]]).await;
        let fan_in = reconnecting_fan_in(addr, Duration::from_secs(1));
        let (tx, mut rx) = mpsc::channel(8);
        let cursor = fan_in
            .consume_once(Some(0), &tx)
            .await
            .expect("consume gap");
        drop(tx);
        let mut rows = Vec::new();
        while let Some(event) = rx.recv().await {
            rows.push(event.source_seq);
        }
        assert_eq!(rows, vec![1, 3]);
        assert_eq!(cursor, Some(3));
        assert_eq!(fan_in.health().state, SourceHealthState::Live);
        assert!(!fan_in
            .health_history()
            .contains(&SourceHealthState::GapDetected));
    }

    #[tokio::test]
    async fn rejected_epoch_install_retries_and_resumes_from_latest_acknowledged_watermark() {
        let bootstrap_calls = Arc::new(AtomicUsize::new(0));
        let calls = bootstrap_calls.clone();
        let bootstrap = move || {
            let calls = calls.clone();
            async move {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                let (epoch, watermark) = if call == 0 {
                    ("epoch-b", 1)
                } else {
                    ("epoch-c", 2)
                };
                Json(json!({
                    "source_epoch": epoch, "high_watermark": watermark,
                    "records": { "sessions": [], "approvals": [], "teams": [],
                        "team_members": [], "team_messages": [], "team_deliveries": [],
                        "managed_worktrees": [], "managed_worktree_claims": [], "processes": [] }
                }))
            }
        };
        let stream = || async {
            let payload = serde_json::to_string(&runtime_event(3)).unwrap();
            Sse::new(tokio_stream::iter(vec![Ok::<_, std::convert::Infallible>(
                Event::default().id("3").data(payload),
            )]))
        };
        let app = Router::new()
            .route("/v1/bootstrap", get(bootstrap))
            .route("/v1/events/stream", get(stream));
        let addr = serve_reconnect_router(app).await;
        let fan_in = reconnecting_fan_in(addr, Duration::from_secs(1));
        let mut changes = fan_in.subscribe_epoch_changes();
        let acknowledger = tokio::spawn(async move {
            let first = changes.recv().await.expect("epoch B change");
            assert_eq!(first.source_epoch, "epoch-b");
            first.reject();
            let second = changes.recv().await.expect("epoch C change");
            assert_eq!(second.source_epoch, "epoch-c");
            second.acknowledge("epoch-c".into(), 2);
        });
        let (tx, mut rx) = mpsc::channel(8);
        let task = fan_in.clone().spawn(Some(9), tx);
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("latest epoch event")
            .expect("event");
        task.abort();
        acknowledger.await.unwrap();
        assert_eq!(event.source_epoch, "epoch-c");
        assert_eq!(event.source_seq, 3);
        assert_eq!(fan_in.health().last_source_seq, Some(3));
    }

    fn reconnecting_fan_in(addr: SocketAddr, stale_after: Duration) -> RuntimeSseFanIn {
        let client = GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
            "local",
            format!("http://{addr}"),
            None,
        ))
        .expect("client");
        RuntimeSseFanIn::new(
            client,
            "epoch-test",
            RuntimeSseFanInConfig {
                reconnect_delay: Duration::from_millis(15),
                stale_after,
                ..RuntimeSseFanInConfig::default()
            },
        )
    }

    async fn spawn_reconnect_sequence_mock(sequences: Vec<Vec<i64>>) -> SocketAddr {
        let calls = Arc::new(AtomicUsize::new(0));
        let stream = move || {
            let calls = calls.clone();
            let sequences = sequences.clone();
            async move {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                let rows = sequences
                    .get(call)
                    .cloned()
                    .or_else(|| sequences.last().cloned())
                    .unwrap_or_default();
                let stream = tokio_stream::iter(rows.into_iter().map(|row_id| {
                    let payload = serde_json::to_string(&runtime_event(row_id)).unwrap();
                    Ok::<_, std::convert::Infallible>(
                        Event::default().id(row_id.to_string()).data(payload),
                    )
                }));
                Sse::new(stream)
            }
        };
        let app = Router::new()
            .route("/v1/bootstrap", get(reconnect_bootstrap))
            .route("/v1/events/stream", get(stream));
        serve_reconnect_router(app).await
    }

    async fn spawn_stale_stream_mock() -> SocketAddr {
        let stream = || async {
            Sse::new(tokio_stream::pending::<
                Result<Event, std::convert::Infallible>,
            >())
        };
        let app = Router::new()
            .route("/v1/bootstrap", get(reconnect_bootstrap))
            .route("/v1/events/stream", get(stream));
        serve_reconnect_router(app).await
    }

    async fn reconnect_bootstrap() -> Json<serde_json::Value> {
        Json(json!({
            "source_epoch": "epoch-test",
            "high_watermark": 0,
            "records": {
                "sessions": [], "approvals": [], "teams": [], "team_members": [],
                "team_messages": [], "team_deliveries": [], "managed_worktrees": [],
                "managed_worktree_claims": [], "processes": []
            }
        }))
    }

    async fn serve_reconnect_router(app: Router) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("mock server") });
        addr
    }

    async fn spawn_decode_then_session_mock(calls: Arc<Mutex<usize>>) -> SocketAddr {
        let route = move || {
            let calls = calls.clone();
            async move {
                let mut calls = calls.lock().unwrap();
                *calls += 1;
                let call_index = *calls;
                drop(calls);

                let event = if call_index == 1 {
                    Event::default().id("1").event("broken").data("{not json")
                } else {
                    let payload = serde_json::to_string(&session_created_event()).unwrap();
                    Event::default()
                        .id("1")
                        .event("session.created")
                        .data(payload)
                };
                let stream = tokio_stream::iter(vec![Ok::<_, std::convert::Infallible>(event)]);
                Sse::new(stream)
            }
        };

        let app = Router::new().route("/v1/events/stream", get(route));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });
        addr
    }

    async fn spawn_sse_mock(observed: Arc<Mutex<Vec<(Option<i64>, Option<i64>)>>>) -> SocketAddr {
        #[derive(Debug, Deserialize)]
        struct StreamQuery {
            after_seq: Option<i64>,
        }

        let route = move |headers: HeaderMap, Query(query): Query<StreamQuery>| {
            let observed = observed.clone();
            async move {
                let last_event_id = headers
                    .get("last-event-id")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<i64>().ok());
                assert_eq!(
                    headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer runtime-token")
                );
                observed
                    .lock()
                    .unwrap()
                    .push((query.after_seq, last_event_id));

                let rows = match query.after_seq {
                    Some(1) => vec![2, 3],
                    Some(3) => vec![3, 4],
                    _ => vec![1],
                };
                let stream = tokio_stream::iter(rows.into_iter().map(|row_id| {
                    let payload = serde_json::to_string(&runtime_event(row_id)).unwrap();
                    Ok::<_, std::convert::Infallible>(
                        Event::default()
                            .id(row_id.to_string())
                            .event("runtime")
                            .data(payload),
                    )
                }));
                Sse::new(stream)
            }
        };

        let app = Router::new().route("/v1/events/stream", get(route));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });
        addr
    }

    fn runtime_event(row_id: i64) -> RuntimeEventRecord {
        RuntimeEventRecord {
            row_id,
            event_id: format!("event_{row_id}"),
            scope: RuntimeEventScope::Session,
            scope_id: "session_1".to_string(),
            session_id: Some("session_1".to_string()),
            team_id: None,
            turn_id: None,
            seq: row_id + 100,
            kind: "turn.text_delta".to_string(),
            criticality: RuntimeEventCriticality::Droppable,
            payload: json!({ "row": row_id }),
            provider: Some("codex".to_string()),
            provider_seq: Some(row_id),
            created_at: row_id,
        }
    }

    fn session_created_event() -> RuntimeEventRecord {
        RuntimeEventRecord {
            row_id: 1,
            event_id: "event_1".to_string(),
            scope: RuntimeEventScope::Session,
            scope_id: "sess_live".to_string(),
            session_id: Some("sess_live".to_string()),
            team_id: None,
            turn_id: None,
            seq: 1,
            kind: "session.created".to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: json!({ "provider": "codex" }),
            provider: Some("codex".to_string()),
            provider_seq: Some(1),
            created_at: 1,
        }
    }
}
