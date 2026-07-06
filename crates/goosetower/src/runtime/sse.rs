use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT};
use runtime_core::RuntimeEventRecord;
use tokio::sync::{mpsc, watch};

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
    config: RuntimeSseFanInConfig,
    health_tx: watch::Sender<SourceHealth>,
}

impl RuntimeSseFanIn {
    pub fn new(client: GooselakeRuntimeClient, config: RuntimeSseFanInConfig) -> Self {
        let source_id = client.source_id().to_string();
        let source_epoch = client.source_epoch().to_string();
        let (health_tx, _) = watch::channel(SourceHealth::new(source_id, source_epoch));
        Self {
            client,
            config,
            health_tx,
        }
    }

    pub fn health(&self) -> SourceHealth {
        self.health_tx.borrow().clone()
    }

    pub fn subscribe_health(&self) -> watch::Receiver<SourceHealth> {
        self.health_tx.subscribe()
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
        let mut seen = HashSet::<i64>::new();

        loop {
            self.transition(SourceHealthState::Replaying, cursor, None);
            match self.consume_once(cursor, &output, &mut seen).await {
                Ok(last_seq) => {
                    cursor = last_seq.or(cursor);
                    self.transition(SourceHealthState::Stale, cursor, None);
                }
                Err(error) => {
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
        seen_source_seqs: &mut HashSet<i64>,
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

        let mut request = self.client.http().get(url).headers(headers);
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
            for frame in parser.push(&chunk) {
                if frame.data.trim().is_empty() {
                    continue;
                }
                let runtime_event = serde_json::from_str::<RuntimeEventRecord>(&frame.data)
                    .map_err(RuntimeClientError::Json)?;
                let source_seq = runtime_event.row_id;
                if let Some(previous) = last_seq {
                    if source_seq > previous + 1 {
                        self.transition(SourceHealthState::GapDetected, Some(previous), None);
                    }
                }
                last_seq = Some(source_seq);
                if !seen_source_seqs.insert(source_seq) {
                    continue;
                }
                let source_event = SourceEvent::from_runtime_event(
                    self.client.source_id().to_string(),
                    self.client.source_epoch().to_string(),
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

    fn transition(
        &self,
        state: SourceHealthState,
        last_source_seq: Option<i64>,
        error: Option<String>,
    ) {
        let mut health = self.health_tx.borrow().clone();
        health.transition(state, last_source_seq, error);
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
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::runtime::{GooselakeRuntimeClient, GooselakeRuntimeClientConfig};
    use axum::extract::Query;
    use axum::http::{header, HeaderMap};
    use axum::response::sse::{Event, Sse};
    use axum::routing::get;
    use axum::Router;
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
            "epoch-test",
            format!("http://{addr}"),
            Some("runtime-token".to_string()),
        ))
        .expect("client");
        let fan_in = RuntimeSseFanIn::new(client, RuntimeSseFanInConfig::default());
        let (tx, mut rx) = mpsc::channel(8);
        let mut seen = HashSet::new();

        let first = fan_in
            .consume_once(Some(1), &tx, &mut seen)
            .await
            .expect("first consume");
        let second = fan_in
            .consume_once(first, &tx, &mut seen)
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
}
