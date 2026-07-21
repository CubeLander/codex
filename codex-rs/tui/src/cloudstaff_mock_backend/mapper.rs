use super::SnapshotMessage;
use super::invalid_data;
use super::required_str;
use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::AgentMessageDeltaNotification;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_protocol::ThreadId;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use tokio::sync::mpsc;

pub(super) struct EventMapper {
    thread_id: ThreadId,
    pub(super) last_event_seq: u64,
    turns: HashMap<String, LiveTurn>,
    completed_turns: HashSet<String>,
}

struct LiveTurn {
    started: bool,
    command_id: String,
    phase: LivePhase,
    user_message_id: Option<String>,
    user_text: String,
    assistant_message_id: Option<String>,
    assistant_text: String,
    assistant_started: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LivePhase {
    User,
    Assistant,
}

impl LiveTurn {
    fn new(command_id: String) -> Self {
        Self {
            started: false,
            command_id,
            phase: LivePhase::User,
            user_message_id: None,
            user_text: String::new(),
            assistant_message_id: None,
            assistant_text: String::new(),
            assistant_started: false,
        }
    }
}

impl EventMapper {
    pub(super) fn new(thread_id: ThreadId, last_event_seq: u64) -> Self {
        Self {
            thread_id,
            last_event_seq,
            turns: HashMap::new(),
            completed_turns: HashSet::new(),
        }
    }

    pub(super) async fn forward_envelope(
        &mut self,
        envelope: &Value,
        event_tx: &mpsc::Sender<AppServerEvent>,
    ) -> Result<()> {
        if envelope.get("type").and_then(Value::as_str) != Some("session.event") {
            return Err(invalid_data("expected session.event envelope"));
        }
        let event_seq = envelope
            .get("eventSeq")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid_data("session.event omitted eventSeq"))?;
        if event_seq <= self.last_event_seq {
            return Ok(());
        }
        if event_seq != self.last_event_seq + 1 {
            return Err(invalid_data("CloudStaff mock event sequence has a gap"));
        }
        let event = envelope
            .get("event")
            .ok_or_else(|| invalid_data("session.event omitted event"))?;
        let notifications = self.map_event(event)?;
        self.last_event_seq = event_seq;
        for notification in notifications {
            event_tx
                .try_send(AppServerEvent::ServerNotification(notification))
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => Error::new(
                        ErrorKind::WouldBlock,
                        "CloudStaff mock event buffer is full",
                    ),
                    mpsc::error::TrySendError::Closed(_) => {
                        Error::new(ErrorKind::BrokenPipe, "TUI event receiver closed")
                    }
                })?;
        }
        Ok(())
    }

    fn map_event(&mut self, event: &Value) -> Result<Vec<ServerNotification>> {
        let kind = required_str(event, "type")?;
        let turn_id = required_str(event, "turnId")?.to_string();
        let message_id = required_str(event, "messageId")?.to_string();
        let command_id = required_str(event, "commandId")?.to_string();
        if self.completed_turns.contains(&turn_id) {
            return Err(invalid_data(
                "CloudStaff mock emitted an event after turn completion",
            ));
        }
        let thread_id = self.thread_id.to_string();
        let live = self
            .turns
            .entry(turn_id.clone())
            .or_insert_with(|| LiveTurn::new(command_id.clone()));
        if live.command_id != command_id {
            return Err(invalid_data(
                "CloudStaff mock changed commandId within a turn",
            ));
        }
        let mut notifications = Vec::new();
        if !live.started {
            live.started = true;
            notifications.push(ServerNotification::TurnStarted(TurnStartedNotification {
                thread_id: thread_id.clone(),
                turn: turn(&turn_id, TurnStatus::InProgress, Vec::new()),
            }));
        }
        match kind {
            "user.message.delta" => {
                require_phase(live.phase, LivePhase::User)?;
                require_stable_id(&mut live.user_message_id, &message_id, "user")?;
                live.user_text.push_str(required_str(event, "delta")?);
            }
            "user.message.completed" => {
                require_phase(live.phase, LivePhase::User)?;
                require_stable_id(&mut live.user_message_id, &message_id, "user")?;
                live.user_text = required_str(event, "text")?.to_string();
                live.phase = LivePhase::Assistant;
                let item = user_item(message_id, live.user_text.clone());
                notifications.push(ServerNotification::ItemStarted(ItemStartedNotification {
                    item: item.clone(),
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    started_at_ms: 0,
                }));
                notifications.push(ServerNotification::ItemCompleted(
                    ItemCompletedNotification {
                        item,
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        completed_at_ms: 0,
                    },
                ));
            }
            "assistant.message.delta" => {
                require_phase(live.phase, LivePhase::Assistant)?;
                require_stable_id(&mut live.assistant_message_id, &message_id, "assistant")?;
                if !live.assistant_started {
                    live.assistant_started = true;
                    notifications.push(ServerNotification::ItemStarted(ItemStartedNotification {
                        item: agent_item(message_id.clone(), String::new()),
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        started_at_ms: 0,
                    }));
                }
                let delta = required_str(event, "delta")?.to_string();
                live.assistant_text.push_str(&delta);
                notifications.push(ServerNotification::AgentMessageDelta(
                    AgentMessageDeltaNotification {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        item_id: message_id,
                        delta,
                    },
                ));
            }
            "assistant.message.completed" => {
                require_phase(live.phase, LivePhase::Assistant)?;
                require_stable_id(&mut live.assistant_message_id, &message_id, "assistant")?;
                let text = required_str(event, "text")?.to_string();
                if !live.assistant_started {
                    notifications.push(ServerNotification::ItemStarted(ItemStartedNotification {
                        item: agent_item(message_id.clone(), String::new()),
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        started_at_ms: 0,
                    }));
                }
                notifications.push(ServerNotification::ItemCompleted(
                    ItemCompletedNotification {
                        item: agent_item(message_id, text.clone()),
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        completed_at_ms: 0,
                    },
                ));
                notifications.push(ServerNotification::TurnCompleted(
                    TurnCompletedNotification {
                        thread_id,
                        turn: turn(
                            &turn_id,
                            TurnStatus::Completed,
                            vec![
                                user_item(
                                    live.user_message_id
                                        .clone()
                                        .unwrap_or_else(|| format!("user-{turn_id}")),
                                    live.user_text.clone(),
                                ),
                                agent_item(
                                    live.assistant_message_id
                                        .clone()
                                        .unwrap_or_else(|| format!("assistant-{turn_id}")),
                                    text,
                                ),
                            ],
                        ),
                    },
                ));
                self.turns.remove(&turn_id);
                self.completed_turns.insert(turn_id);
            }
            _ => return Err(invalid_data("unsupported CloudStaff mock event type")),
        }
        Ok(notifications)
    }
}

fn require_phase(actual: LivePhase, expected: LivePhase) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(invalid_data("CloudStaff mock event arrived out of order"))
    }
}

fn require_stable_id(current: &mut Option<String>, incoming: &str, role: &str) -> Result<()> {
    match current {
        Some(current) if current != incoming => Err(invalid_data(format!(
            "CloudStaff mock changed {role} messageId within a turn"
        ))),
        Some(_) => Ok(()),
        None => {
            *current = Some(incoming.to_string());
            Ok(())
        }
    }
}

pub(super) fn snapshot_turns(messages: &[SnapshotMessage]) -> Vec<Turn> {
    let mut turns = Vec::<Turn>::new();
    let mut indices = HashMap::<&str, usize>::new();
    for message in messages.iter().filter(|message| message.completed) {
        let index = if let Some(index) = indices.get(message.turn_id.as_str()) {
            *index
        } else {
            let index = turns.len();
            indices.insert(message.turn_id.as_str(), index);
            turns.push(turn(&message.turn_id, TurnStatus::Completed, Vec::new()));
            index
        };
        let item = match message.role.as_str() {
            "user" => user_item(message.message_id.clone(), message.text.clone()),
            "assistant" => agent_item(message.message_id.clone(), message.text.clone()),
            _ => continue,
        };
        turns[index].items.push(item);
    }
    turns
}

fn turn(id: &str, status: TurnStatus, items: Vec<ThreadItem>) -> Turn {
    Turn {
        id: id.to_string(),
        items,
        items_view: TurnItemsView::Full,
        status,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
    }
}

pub(super) fn turn_value(id: &str, status: TurnStatus) -> Result<Value> {
    serde_json::to_value(turn(id, status, Vec::new()))
        .map_err(|error| invalid_data(format!("failed to encode mapped turn: {error}")))
}

fn user_item(id: String, text: String) -> ThreadItem {
    ThreadItem::UserMessage {
        id,
        client_id: None,
        content: vec![UserInput::Text {
            text,
            text_elements: Vec::new(),
        }],
    }
}

fn agent_item(id: String, text: String) -> ThreadItem {
    ThreadItem::AgentMessage {
        id,
        text,
        phase: None,
        memory_citation: None,
    }
}
