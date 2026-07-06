use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use runtime_core::RuntimeError;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{oneshot, Mutex, RwLock};

use crate::config::DEFAULT_PROTOCOL_VERSION;
use crate::protocol::{jsonrpc_error_message, message_id_key, parse_initialize_capabilities};
use crate::provider::AcpProvider;
use crate::state::AcpAgentCapabilities;

const STDERR_TAIL_MAX_BYTES: usize = 8 * 1024;

#[derive(Debug)]
pub(super) struct AcpConnection {
    pub(super) child: Mutex<Child>,
    pub(super) stdin: Mutex<BufWriter<ChildStdin>>,
    pub(super) pending_requests:
        Mutex<HashMap<String, oneshot::Sender<Result<Value, RuntimeError>>>>,
    pub(super) next_request_id: AtomicU64,
    pub(super) closed: AtomicBool,
    pub(super) capabilities: RwLock<AcpAgentCapabilities>,
    pub(super) stderr_tail: Mutex<String>,
}

impl AcpConnection {
    pub(super) async fn spawn(provider: AcpProvider) -> Result<Arc<Self>, RuntimeError> {
        let command = provider.configured_command()?;

        let mut child = Command::new(command.as_str());
        child.args(provider.inner.config.args.iter());
        child.stdin(Stdio::piped());
        child.stdout(Stdio::piped());
        child.stderr(Stdio::piped());
        for (key, value) in &provider.inner.config.env {
            child.env(key, value);
        }

        let mut child = child
            .spawn()
            .map_err(|error| RuntimeError::Io(format!("failed to spawn acp agent: {error}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::Io("acp agent did not expose stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::Io("acp agent did not expose stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| RuntimeError::Io("acp agent did not expose stderr".to_string()))?;

        let connection = Arc::new(Self {
            child: Mutex::new(child),
            stdin: Mutex::new(BufWriter::new(stdin)),
            pending_requests: Mutex::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            capabilities: RwLock::new(AcpAgentCapabilities::default()),
            stderr_tail: Mutex::new(String::new()),
        });

        connection.spawn_reader(provider.clone(), stdout);
        connection.spawn_stderr_reader(stderr);

        let init_result = connection
            .send_request(
                "initialize",
                json!({
                    "protocolVersion": DEFAULT_PROTOCOL_VERSION,
                    "clientCapabilities": {
                        "fs": {
                            "readTextFile": false,
                            "writeTextFile": false
                        },
                        "terminal": false
                    },
                    "clientInfo": {
                        "name": "gg-runtime",
                        "title": "GG Runtime",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
                Some(provider.request_timeout()),
            )
            .await;
        let init_result = match init_result {
            Ok(init_result) => init_result,
            Err(error) => {
                connection.shutdown(true).await;
                return Err(error);
            }
        };
        let capabilities = match parse_initialize_capabilities(&init_result) {
            Ok(capabilities) => capabilities,
            Err(error) => {
                connection.shutdown(true).await;
                return Err(error);
            }
        };
        *connection.capabilities.write().await = capabilities;

        Ok(connection)
    }

    fn spawn_reader(self: &Arc<Self>, provider: AcpProvider, stdout: ChildStdout) {
        let connection = Arc::clone(self);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        let parsed = serde_json::from_str::<Value>(line.as_str());
                        let message = match parsed {
                            Ok(message) => message,
                            Err(error) => {
                                connection
                                    .mark_closed_protocol(format!(
                                        "acp agent emitted malformed JSON-RPC line: {error}"
                                    ))
                                    .await;
                                break;
                            }
                        };

                        match message.get("method").and_then(Value::as_str) {
                            Some("session/update") => {
                                let session_id = message
                                    .get("params")
                                    .and_then(|params| params.get("sessionId"))
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                let update = message
                                    .get("params")
                                    .and_then(|params| params.get("update"))
                                    .cloned();
                                if let (Some(session_id), Some(update)) = (session_id, update) {
                                    let _ = provider
                                        .apply_session_update(session_id.as_str(), update)
                                        .await;
                                }
                            }
                            Some("session/request_permission") => {
                                let session_id = message
                                    .get("params")
                                    .and_then(|params| params.get("sessionId"))
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                let request_id = message.get("id").cloned();
                                if let Some(request_id) = request_id {
                                    let _ = connection
                                        .write_message(&json!({
                                            "jsonrpc": "2.0",
                                            "id": request_id,
                                            "result": {
                                                "outcome": {
                                                    "outcome": "cancelled"
                                                }
                                            }
                                        }))
                                        .await;
                                }
                                if let Some(session_id) = session_id {
                                    let _ =
                                        provider.fail_permission_request(session_id.as_str()).await;
                                }
                                continue;
                            }
                            Some(_) => continue,
                            None => {}
                        }

                        if let Some(id_key) = message_id_key(&message) {
                            let responder = {
                                let mut pending = connection.pending_requests.lock().await;
                                pending.remove(id_key.as_str())
                            };
                            if let Some(responder) = responder {
                                if let Some(error) = message.get("error") {
                                    let _ = responder.send(Err(RuntimeError::ProtocolViolation(
                                        format!(
                                            "acp request {} failed: {}",
                                            id_key,
                                            jsonrpc_error_message(error)
                                        ),
                                    )));
                                } else if let Some(result) = message.get("result") {
                                    let _ = responder.send(Ok(result.clone()));
                                } else {
                                    let _ = responder.send(Err(RuntimeError::ProtocolViolation(
                                        format!("acp response {} missing result and error", id_key),
                                    )));
                                }
                                continue;
                            }
                        }
                    }
                    Ok(None) => {
                        let stderr = connection.stderr_tail.lock().await.clone();
                        let detail = if stderr.trim().is_empty() {
                            "acp agent connection closed".to_string()
                        } else {
                            format!("acp agent connection closed: {}", stderr.trim())
                        };
                        connection.mark_closed_io(detail).await;
                        break;
                    }
                    Err(error) => {
                        connection
                            .mark_closed_io(format!("failed reading acp agent stdout: {error}"))
                            .await;
                        break;
                    }
                }
            }
            provider
                .reap_connection_if_current_and_closed(&connection)
                .await;
        });
    }

    fn spawn_stderr_reader(self: &Arc<Self>, stderr: ChildStderr) {
        let connection = Arc::clone(self);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut tail = connection.stderr_tail.lock().await;
                if !tail.is_empty() {
                    tail.push('\n');
                }
                tail.push_str(line.as_str());
                if tail.len() > STDERR_TAIL_MAX_BYTES {
                    let split_at = tail.len().saturating_sub(STDERR_TAIL_MAX_BYTES);
                    let trimmed = tail.split_off(split_at);
                    *tail = trimmed;
                }
            }
        });
    }

    async fn write_message(&self, message: &Value) -> Result<(), RuntimeError> {
        let mut stdin = self.stdin.lock().await;
        let bytes = serde_json::to_vec(message).map_err(|error| {
            RuntimeError::ProtocolViolation(format!(
                "failed serializing acp json-rpc message: {error}"
            ))
        })?;
        stdin.write_all(bytes.as_slice()).await.map_err(|error| {
            RuntimeError::Io(format!("failed writing to acp agent stdin: {error}"))
        })?;
        stdin.write_all(b"\n").await.map_err(|error| {
            RuntimeError::Io(format!(
                "failed writing newline to acp agent stdin: {error}"
            ))
        })?;
        stdin.flush().await.map_err(|error| {
            RuntimeError::Io(format!("failed flushing acp agent stdin: {error}"))
        })?;
        Ok(())
    }

    pub(super) async fn send_notification(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(), RuntimeError> {
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .await
    }

    pub(super) async fn send_request(
        &self,
        method: &str,
        params: Value,
        timeout: Option<Duration>,
    ) -> Result<Value, RuntimeError> {
        if self.closed.load(Ordering::SeqCst) {
            let stderr = self.stderr_tail.lock().await.clone();
            return Err(RuntimeError::Io(if stderr.trim().is_empty() {
                "acp agent connection is closed".to_string()
            } else {
                format!("acp agent connection is closed: {}", stderr.trim())
            }));
        }

        let id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let id_key = id.to_string();
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(id_key.clone(), sender);
        }

        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        if let Err(error) = self.write_message(&message).await {
            let mut pending = self.pending_requests.lock().await;
            pending.remove(id_key.as_str());
            return Err(error);
        }

        let result = match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, receiver).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(RuntimeError::InvalidState(format!(
                    "acp response channel closed for request {}",
                    id
                ))),
                Err(_) => {
                    let mut pending = self.pending_requests.lock().await;
                    pending.remove(id_key.as_str());
                    Err(RuntimeError::InvalidState(format!(
                        "timed out waiting for acp response to {}",
                        method
                    )))
                }
            },
            None => match receiver.await {
                Ok(result) => result,
                Err(_) => Err(RuntimeError::InvalidState(format!(
                    "acp response channel closed for request {}",
                    id
                ))),
            },
        }?;

        Ok(result)
    }

    async fn mark_closed_io(&self, message: String) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut pending = self.pending_requests.lock().await;
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(RuntimeError::Io(message.clone())));
        }
    }

    async fn mark_closed_protocol(&self, message: String) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut pending = self.pending_requests.lock().await;
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(RuntimeError::ProtocolViolation(message.clone())));
        }
    }

    pub(super) async fn shutdown(&self, kill_if_running: bool) {
        self.closed.store(true, Ordering::SeqCst);
        {
            let mut pending = self.pending_requests.lock().await;
            for (_, sender) in pending.drain() {
                let _ = sender.send(Err(RuntimeError::Io(
                    "acp connection shutting down".to_string(),
                )));
            }
        }
        {
            let mut stdin = self.stdin.lock().await;
            let _ = stdin.flush().await;
        }
        let mut child = self.child.lock().await;
        if kill_if_running {
            let _ = child.start_kill();
        }
        let _ = child.wait().await;
    }
}
