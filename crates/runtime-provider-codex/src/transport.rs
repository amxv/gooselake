use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use runtime_core::RuntimeError;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

use crate::CodexProviderConfig;

fn rpc_id_key(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(format!("s:{value}")),
        Value::Number(value) => Some(format!("n:{value}")),
        _ => None,
    }
}

#[derive(Debug)]
pub(super) struct CodexTransport {
    child: Arc<Mutex<Child>>,
    writer: mpsc::Sender<Value>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, Value>>>>>,
    incoming: broadcast::Sender<Value>,
    next_request_id: AtomicU64,
    request_timeout: Duration,
}

impl CodexTransport {
    pub(super) async fn spawn(config: &CodexProviderConfig) -> Result<Arc<Self>, RuntimeError> {
        tokio::fs::create_dir_all(&config.home_dir)
            .await
            .map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create Codex home {}: {error}",
                    config.home_dir.display()
                ))
            })?;

        let mut command = Command::new(&config.command);
        command
            .args(&config.app_server_args)
            .env("CODEX_HOME", &config.home_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|error| {
            RuntimeError::provider_not_dispatched(
                "codex_app_server_unavailable",
                format!(
                    "failed to spawn Codex app-server command {:?} with args {:?}: {error}",
                    config.command, config.app_server_args
                ),
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::Io("Codex app-server did not expose stdin".to_string()))?;
        let stdout = child.stdout.take().ok_or_else(|| {
            RuntimeError::Io("Codex app-server did not expose stdout".to_string())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            RuntimeError::Io("Codex app-server did not expose stderr".to_string())
        })?;

        let (writer_tx, mut writer_rx) = mpsc::channel::<Value>(256);
        let (incoming_tx, _) = broadcast::channel::<Value>(4096);
        let pending = Arc::new(Mutex::new(HashMap::<
            String,
            oneshot::Sender<Result<Value, Value>>,
        >::new()));
        let child = Arc::new(Mutex::new(child));

        let pending_for_stdout = Arc::clone(&pending);
        let incoming_for_stdout = incoming_tx.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        let parsed = match serde_json::from_str::<Value>(&line) {
                            Ok(parsed) => parsed,
                            Err(error) => {
                                let _ = incoming_for_stdout.send(json!({
                                    "method": "transport/error",
                                    "params": {
                                        "message": format!(
                                            "failed to parse Codex app-server JSON-RPC payload: {error}"
                                        )
                                    }
                                }));
                                continue;
                            }
                        };
                        let response_key = parsed.get("id").and_then(rpc_id_key);
                        let is_response = parsed.get("method").is_none()
                            && (parsed.get("result").is_some() || parsed.get("error").is_some());
                        if is_response {
                            if let Some(key) = response_key {
                                if let Some(sender) = pending_for_stdout.lock().await.remove(&key) {
                                    let result = if let Some(error) = parsed.get("error") {
                                        Err(error.clone())
                                    } else {
                                        Ok(parsed.get("result").cloned().unwrap_or(Value::Null))
                                    };
                                    let _ = sender.send(result);
                                    continue;
                                }
                            }
                        }
                        if parsed.get("method").is_none() {
                            let _ = incoming_for_stdout.send(json!({
                                "method": "transport/error",
                                "params": {
                                    "message": format!(
                                        "unexpected Codex app-server payload shape: {parsed}"
                                    )
                                }
                            }));
                            continue;
                        }
                        let _ = incoming_for_stdout.send(parsed);
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = incoming_for_stdout.send(json!({
                            "method": "transport/error",
                            "params": { "message": error.to_string() }
                        }));
                        break;
                    }
                }
            }

            let mut pending = pending_for_stdout.lock().await;
            for (_, sender) in pending.drain() {
                let _ = sender.send(Err(json!({
                    "code": "codex_stdout_closed",
                    "message": "Codex app-server stdout closed"
                })));
            }
            let _ = incoming_for_stdout.send(json!({
                "method": "transport/closed",
                "params": {}
            }));
        });

        let incoming_for_writer = incoming_tx.clone();
        tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(message) = writer_rx.recv().await {
                let encoded = match serde_json::to_vec(&message) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        let _ = incoming_for_writer.send(json!({
                            "method": "transport/error",
                            "params": { "message": format!("failed to serialize Codex request: {error}") }
                        }));
                        break;
                    }
                };
                let write_result = async {
                    stdin.write_all(&encoded).await?;
                    stdin.write_all(b"\n").await?;
                    stdin.flush().await
                }
                .await;
                if let Err(error) = write_result {
                    let _ = incoming_for_writer.send(json!({
                        "method": "transport/error",
                        "params": { "message": format!("failed writing Codex app-server stdin: {error}") }
                    }));
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(_line)) = lines.next_line().await {
                // Keep stderr drained so the child cannot block. Provider errors are
                // surfaced through the JSON-RPC channel where they can be correlated.
            }
        });

        let transport = Arc::new(Self {
            child,
            writer: writer_tx,
            pending,
            incoming: incoming_tx,
            next_request_id: AtomicU64::new(1),
            request_timeout: Duration::from_millis(config.request_timeout_ms.max(1)),
        });

        transport.initialize().await?;
        Ok(transport)
    }

    async fn initialize(&self) -> Result<(), RuntimeError> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "gooselake",
                    "title": "Gooselake Runtime",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true,
                    "requestAttestation": false,
                },
            }),
        )
        .await?;
        self.notify("initialized", json!({})).await
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.incoming.subscribe()
    }

    pub(super) async fn request(&self, method: &str, params: Value) -> Result<Value, RuntimeError> {
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let id_value = json!(id);
        let key = rpc_id_key(&id_value).expect("numeric RPC id must be representable");
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(key.clone(), sender);

        let message = json!({ "id": id, "method": method, "params": params });
        if self.writer.send(message).await.is_err() {
            self.pending.lock().await.remove(&key);
            return Err(RuntimeError::provider_not_dispatched(
                "codex_transport_closed",
                format!("Codex app-server writer is closed before request {method} was queued"),
            ));
        }

        match tokio::time::timeout(self.request_timeout, receiver).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(error))) => Err(map_rpc_error(method, &error)),
            Ok(Err(_)) => Err(RuntimeError::provider_dispatch_unknown(
                "codex_response_channel_closed",
                format!("Codex app-server response channel closed for {method}"),
            )),
            Err(_) => {
                self.pending.lock().await.remove(&key);
                Err(RuntimeError::provider_dispatch_unknown(
                    "codex_request_timeout",
                    format!("Codex app-server request timed out: {method}"),
                ))
            }
        }
    }

    pub(super) async fn notify(&self, method: &str, params: Value) -> Result<(), RuntimeError> {
        self.writer
            .send(json!({ "method": method, "params": params }))
            .await
            .map_err(|_| {
                RuntimeError::provider_dispatch_unknown(
                    "codex_transport_closed",
                    "Codex app-server writer is closed",
                )
            })
    }

    pub(super) async fn respond(&self, id: Value, result: Value) -> Result<(), RuntimeError> {
        self.writer
            .send(json!({ "id": id, "result": result }))
            .await
            .map_err(|_| {
                RuntimeError::provider_dispatch_unknown(
                    "codex_transport_closed",
                    "Codex app-server writer is closed",
                )
            })
    }

    pub(super) async fn shutdown(&self) {
        let mut child = self.child.lock().await;
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

fn map_rpc_error(method: &str, error: &Value) -> RuntimeError {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Codex app-server request failed");
    if method == "turn/start" && message == "Selected model is at capacity." {
        return RuntimeError::provider_not_dispatched("codex_model_capacity", message);
    }
    RuntimeError::ProtocolViolation(format!("Codex app-server RPC error: {error}"))
}
