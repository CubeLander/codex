//! Experimental CloudStaff UDS/JSONL adapter used only by the fork spike.
//!
//! This speaks the deterministic protocol implemented by
//! `prototypes/cloudstaff-codex-tui/mock_session_server.py`. It is deliberately
//! not an app-server compatibility transport.

use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

use self::mapper::EventMapper;
use self::mapper::snapshot_turns;
use self::mapper::turn_value;
use self::wire::invalid_data;
use self::wire::invalid_wire;
use self::wire::read_json_line;
use self::wire::request_method_name;
use self::wire::required_str;
use self::wire::required_u64;
use self::wire::text_only_input;
use self::wire::wire_error;
use self::wire::write_json_line;

mod mapper;
mod wire;

const MOCK_THREAD_NAMESPACE: Uuid = Uuid::from_u128(0x8a4f6f6d_0c0c_4f71_99b1_99fd30f18f11);
const JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct CloudStaffBackendClient {
    request_handle: CloudStaffBackendRequestHandle,
    event_rx: mpsc::Receiver<AppServerEvent>,
    joined: Arc<JoinedSession>,
    worker: JoinHandle<()>,
}

#[derive(Clone)]
pub(crate) struct CloudStaffBackendRequestHandle {
    command_tx: mpsc::Sender<BackendCommand>,
}

struct JoinedSession {
    session_id: String,
    thread_id: ThreadId,
    snapshot_messages: Vec<SnapshotMessage>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotMessage {
    message_id: String,
    turn_id: String,
    role: String,
    text: String,
    completed: bool,
}

enum BackendCommand {
    Request {
        request: ClientRequest,
        response: oneshot::Sender<Result<Value>>,
    },
    Shutdown {
        completed: oneshot::Sender<()>,
    },
}

impl CloudStaffBackendClient {
    pub(crate) async fn connect(
        socket_path: &Path,
        session_id: String,
        device_id: String,
    ) -> Result<Self> {
        let stream = UnixStream::connect(socket_path).await?;
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        let join = json!({
            "type": "session.join",
            "requestId": "codex-tui-join-1",
            "sessionId": session_id,
            "deviceId": device_id,
            "afterEventSeq": null,
        });
        write_json_line(&mut writer, &join).await?;
        let joined_value = timeout(JOIN_TIMEOUT, read_json_line(&mut lines))
            .await
            .map_err(|_| Error::new(ErrorKind::TimedOut, "CloudStaff mock join timed out"))??
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "mock closed before join"))?;
        if joined_value.get("type").and_then(Value::as_str) != Some("session.joined") {
            return Err(wire_error(&joined_value));
        }

        let joined_session_id = required_str(&joined_value, "sessionId")?.to_string();
        if joined_session_id != session_id {
            return Err(invalid_data("CloudStaff mock joined the wrong session"));
        }
        if required_str(&joined_value, "deviceId")? != device_id {
            return Err(invalid_data("CloudStaff mock echoed the wrong device"));
        }
        required_u64(&joined_value, "hubEpoch")?;
        joined_value
            .get("snapshot")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid_data("CloudStaff mock join omitted snapshot"))?;
        if joined_value
            .pointer("/snapshot/status")
            .and_then(Value::as_str)
            != Some("ready")
        {
            return Err(invalid_data("CloudStaff mock snapshot is not ready"));
        }
        joined_value
            .pointer("/snapshot/primaryThreadId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_data("CloudStaff mock snapshot omitted primaryThreadId"))?;
        let thread_id = deterministic_thread_id(&joined_session_id)?;
        let snapshot_messages = joined_value
            .pointer("/snapshot/messages")
            .cloned()
            .ok_or_else(|| invalid_data("CloudStaff mock snapshot omitted messages"))
            .and_then(|value| serde_json::from_value(value).map_err(invalid_wire))?;
        let joined = Arc::new(JoinedSession {
            session_id: joined_session_id,
            thread_id,
            snapshot_messages,
        });
        let (event_tx, event_rx) = mpsc::channel(256);
        let (command_tx, command_rx) = mpsc::channel(32);
        let replay = joined_value
            .get("replay")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_data("CloudStaff mock join omitted replay"))?;
        if !replay.is_empty() {
            return Err(invalid_data(
                "fresh CloudStaff mock join unexpectedly included replay events",
            ));
        }
        let joined_event_seq = joined_value
            .get("eventSeq")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid_data("session.joined omitted eventSeq"))?;
        let mapper = EventMapper::new(thread_id, joined_event_seq);

        let worker = tokio::spawn(run_worker(
            lines,
            writer,
            command_rx,
            event_tx,
            mapper,
            Arc::clone(&joined),
        ));
        Ok(Self {
            request_handle: CloudStaffBackendRequestHandle { command_tx },
            event_rx,
            joined: Arc::clone(&joined),
            worker,
        })
    }

    pub(crate) fn request_handle(&self) -> CloudStaffBackendRequestHandle {
        self.request_handle.clone()
    }

    pub(crate) async fn next_event(&mut self) -> Option<AppServerEvent> {
        self.event_rx.recv().await
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        let (completed, wait) = oneshot::channel();
        let graceful = timeout(
            SHUTDOWN_TIMEOUT,
            self.request_handle
                .command_tx
                .send(BackendCommand::Shutdown { completed }),
        )
        .await
        .is_ok_and(|result| result.is_ok())
            && timeout(SHUTDOWN_TIMEOUT, wait)
                .await
                .is_ok_and(|result| result.is_ok());
        if !graceful {
            self.worker.abort();
        }
        match timeout(SHUTDOWN_TIMEOUT, &mut self.worker).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if error.is_cancelled() && !graceful => Ok(()),
            Ok(Err(error)) => Err(Error::other(format!(
                "CloudStaff mock worker failed: {error}"
            ))),
            Err(_) => {
                self.worker.abort();
                let _ = self.worker.await;
                Err(Error::new(
                    ErrorKind::TimedOut,
                    "CloudStaff mock worker did not shut down",
                ))
            }
        }
    }

    pub(crate) fn attached_response(
        &self,
        model: String,
        cwd: AbsolutePathBuf,
    ) -> ThreadResumeResponse {
        self.joined.attached_response(model, cwd)
    }
}

impl CloudStaffBackendRequestHandle {
    pub(crate) async fn request(&self, request: ClientRequest) -> Result<Value> {
        match &request {
            ClientRequest::ThreadUnsubscribe { .. } => return Ok(json!({})),
            // The mock Worker exposes no skills. Production must project the
            // backend-owned catalog through the message plane instead.
            ClientRequest::SkillsList { .. } => return Ok(json!({ "data": [] })),
            ClientRequest::ThreadResume { .. } => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "CloudStaff attach is injected at startup; thread/resume is forbidden",
                ));
            }
            ClientRequest::TurnStart { .. } => {}
            _ => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!(
                        "{} is not admitted by the CloudStaff mock adapter",
                        request_method_name(&request)
                    ),
                ));
            }
        }
        let (response, wait) = oneshot::channel();
        self.command_tx
            .send(BackendCommand::Request { request, response })
            .await
            .map_err(|_| Error::new(ErrorKind::BrokenPipe, "CloudStaff mock worker stopped"))?;
        wait.await
            .map_err(|_| Error::new(ErrorKind::BrokenPipe, "CloudStaff mock response was lost"))?
    }
}

impl JoinedSession {
    fn attached_response(&self, model: String, cwd: AbsolutePathBuf) -> ThreadResumeResponse {
        let turns = snapshot_turns(&self.snapshot_messages);
        let preview = self
            .snapshot_messages
            .iter()
            .find(|message| message.role == "user")
            .map(|message| message.text.clone())
            .unwrap_or_default();
        ThreadResumeResponse {
            thread: Thread {
                id: self.thread_id.to_string(),
                extra: None,
                session_id: self.session_id.clone(),
                forked_from_id: None,
                parent_thread_id: None,
                preview,
                ephemeral: true,
                history_mode: Default::default(),
                model_provider: "cloudstaff".to_string(),
                created_at: 0,
                updated_at: 0,
                recency_at: None,
                status: ThreadStatus::Idle,
                path: None,
                cwd: cwd.clone(),
                cli_version: env!("CARGO_PKG_VERSION").to_string(),
                source: codex_app_server_protocol::SessionSource::Custom(
                    "cloudstaff-mock".to_string(),
                ),
                can_accept_direct_input: Some(true),
                thread_source: None,
                agent_nickname: Some("alice".to_string()),
                agent_role: None,
                git_info: None,
                name: Some("Alice".to_string()),
                turns,
            },
            model,
            model_provider: "cloudstaff".to_string(),
            service_tier: None,
            cwd,
            runtime_workspace_roots: Vec::new(),
            instruction_sources: Vec::new(),
            approval_policy: codex_app_server_protocol::AskForApproval::Never,
            approvals_reviewer: codex_app_server_protocol::ApprovalsReviewer::User,
            sandbox: codex_app_server_protocol::SandboxPolicy::ReadOnly {
                network_access: false,
            },
            active_permission_profile: None,
            reasoning_effort: None,
            multi_agent_mode: Default::default(),
            initial_turns_page: None,
            turns_backwards_cursor: None,
            items_backwards_cursor: None,
        }
    }
}

async fn run_worker(
    mut lines: tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    mut writer: tokio::net::unix::OwnedWriteHalf,
    mut command_rx: mpsc::Receiver<BackendCommand>,
    event_tx: mpsc::Sender<AppServerEvent>,
    mut mapper: EventMapper,
    joined: Arc<JoinedSession>,
) {
    loop {
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    Some(BackendCommand::Request { request, response }) => {
                        let result = timeout(
                            COMMAND_TIMEOUT,
                            submit_turn(
                                &mut lines,
                                &mut writer,
                                &event_tx,
                                &mut mapper,
                                &joined,
                                request,
                            ),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            Err(Error::new(
                                ErrorKind::TimedOut,
                                "CloudStaff mock command acknowledgement timed out",
                            ))
                        });
                        let failed = result.is_err();
                        let _ = response.send(result);
                        if failed {
                            let _ = event_tx.try_send(AppServerEvent::Disconnected {
                                message: "CloudStaff mock command failed; reconnect required".to_string(),
                            });
                            break;
                        }
                    }
                    Some(BackendCommand::Shutdown { completed }) => {
                        let _ = writer.shutdown().await;
                        let _ = completed.send(());
                        break;
                    }
                    None => break,
                }
            }
            line = read_json_line(&mut lines) => {
                match line {
                    Ok(Some(value)) => {
                        if let Err(err) = mapper.forward_envelope(&value, &event_tx).await {
                            let _ = event_tx.try_send(AppServerEvent::Disconnected {
                                message: format!("CloudStaff mock protocol error: {err}"),
                            });
                            break;
                        }
                    }
                    Ok(None) => {
                        let _ = event_tx.try_send(AppServerEvent::Disconnected {
                            message: "CloudStaff mock session socket closed".to_string(),
                        });
                        break;
                    }
                    Err(err) => {
                        let _ = event_tx.try_send(AppServerEvent::Disconnected {
                            message: format!("CloudStaff mock session read failed: {err}"),
                        });
                        break;
                    }
                }
            }
        }
    }
}

async fn submit_turn(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    event_tx: &mpsc::Sender<AppServerEvent>,
    mapper: &mut EventMapper,
    joined: &JoinedSession,
    request: ClientRequest,
) -> Result<Value> {
    let ClientRequest::TurnStart { request_id, params } = request else {
        return Err(Error::new(ErrorKind::Unsupported, "unsupported command"));
    };
    let command_id = params.client_user_message_id.ok_or_else(|| {
        invalid_data("CloudStaff turn/start requires a stable clientUserMessageId")
    })?;
    let text = text_only_input(params.input)?;
    let request_id = format!("codex-{request_id:?}");
    write_json_line(
        writer,
        &json!({
            "type": "session.command",
            "requestId": request_id,
            "commandId": command_id,
            "commandKind": "turn.start",
            "text": text,
        }),
    )
    .await?;

    loop {
        let value = read_json_line(lines)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "mock closed during command"))?;
        match value.get("type").and_then(Value::as_str) {
            Some("session.command.accepted")
                if value.get("requestId").and_then(Value::as_str) == Some(request_id.as_str()) =>
            {
                if required_str(&value, "sessionId")? != joined.session_id {
                    return Err(invalid_data(
                        "CloudStaff mock accepted a command for the wrong session",
                    ));
                }
                if required_str(&value, "commandId")? != command_id {
                    return Err(invalid_data("CloudStaff mock accepted the wrong commandId"));
                }
                let turn_id = required_str(&value, "turnId")?;
                let first_event_seq = required_u64(&value, "firstEventSeq")?;
                let last_event_seq = required_u64(&value, "lastEventSeq")?;
                if first_event_seq > last_event_seq {
                    return Err(invalid_data(
                        "CloudStaff mock accepted an invalid event range",
                    ));
                }
                let duplicate = value
                    .get("duplicate")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| invalid_data("mock message omitted duplicate"))?;
                if duplicate {
                    if last_event_seq > mapper.last_event_seq {
                        return Err(invalid_data(
                            "duplicate CloudStaff command references unseen events",
                        ));
                    }
                } else if first_event_seq != mapper.last_event_seq + 1 {
                    return Err(invalid_data(
                        "CloudStaff command event range is not contiguous",
                    ));
                }
                return Ok(json!({
                    "turn": turn_value(turn_id, TurnStatus::InProgress)?,
                }));
            }
            Some("error")
                if value.get("requestId").and_then(Value::as_str) == Some(request_id.as_str()) =>
            {
                return Err(wire_error(&value));
            }
            Some("session.event") => mapper.forward_envelope(&value, event_tx).await?,
            _ => {
                return Err(invalid_data(
                    "unexpected mock response while submitting command",
                ));
            }
        }
    }
}

fn deterministic_thread_id(session_id: &str) -> Result<ThreadId> {
    ThreadId::from_string(&Uuid::new_v5(&MOCK_THREAD_NAMESPACE, session_id.as_bytes()).to_string())
        .map_err(invalid_wire)
}

#[cfg(test)]
#[path = "cloudstaff_mock_backend_tests.rs"]
mod tests;
