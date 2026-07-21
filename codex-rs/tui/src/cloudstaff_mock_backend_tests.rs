use super::*;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadItem;

#[test]
fn snapshot_messages_become_one_completed_turn() {
    let messages = vec![
        SnapshotMessage {
            message_id: "user-1".to_string(),
            turn_id: "turn-1".to_string(),
            role: "user".to_string(),
            text: "hello".to_string(),
            completed: true,
        },
        SnapshotMessage {
            message_id: "assistant-1".to_string(),
            turn_id: "turn-1".to_string(),
            role: "assistant".to_string(),
            text: "hi".to_string(),
            completed: true,
        },
    ];

    let turns = snapshot_turns(&messages);

    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].id, "turn-1");
    assert_eq!(turns[0].status, TurnStatus::Completed);
    assert!(matches!(turns[0].items[0], ThreadItem::UserMessage { .. }));
    assert!(matches!(turns[0].items[1], ThreadItem::AgentMessage { .. }));
}

#[tokio::test]
async fn live_events_map_to_stock_renderer_lifecycle_and_fence_duplicates() {
    let thread_id = deterministic_thread_id("alice").expect("thread id");
    let mut mapper = EventMapper::new(thread_id, 0);
    let (tx, mut rx) = mpsc::channel(16);
    let events = [
        json!({
            "type": "user.message.delta",
            "commandId": "command-1",
            "turnId": "turn-1",
            "messageId": "user-1",
            "delta": "hello",
        }),
        json!({
            "type": "user.message.completed",
            "commandId": "command-1",
            "turnId": "turn-1",
            "messageId": "user-1",
            "text": "hello",
        }),
        json!({
            "type": "assistant.message.delta",
            "commandId": "command-1",
            "turnId": "turn-1",
            "messageId": "assistant-1",
            "delta": "hi",
        }),
        json!({
            "type": "assistant.message.completed",
            "commandId": "command-1",
            "turnId": "turn-1",
            "messageId": "assistant-1",
            "text": "hi",
        }),
    ];
    for (index, event) in events.iter().enumerate() {
        mapper
            .forward_envelope(
                &json!({
                    "type": "session.event",
                    "eventSeq": index + 1,
                    "event": event,
                }),
                &tx,
            )
            .await
            .expect("event should map");
    }

    let mut notifications = Vec::new();
    while let Ok(event) = rx.try_recv() {
        let AppServerEvent::ServerNotification(notification) = event else {
            panic!("expected renderer notification");
        };
        notifications.push(notification);
    }
    assert_eq!(notifications.len(), 7);
    assert!(matches!(
        notifications[0],
        ServerNotification::TurnStarted(_)
    ));
    assert!(matches!(
        notifications[1],
        ServerNotification::ItemStarted(_)
    ));
    assert!(matches!(
        notifications[2],
        ServerNotification::ItemCompleted(_)
    ));
    assert!(matches!(
        notifications[3],
        ServerNotification::ItemStarted(_)
    ));
    assert!(matches!(
        notifications[4],
        ServerNotification::AgentMessageDelta(_)
    ));
    assert!(matches!(
        notifications[5],
        ServerNotification::ItemCompleted(_)
    ));
    assert!(matches!(
        notifications[6],
        ServerNotification::TurnCompleted(_)
    ));

    mapper
        .forward_envelope(
            &json!({
                "type": "session.event",
                "eventSeq": 4,
                "event": events[3],
            }),
            &tx,
        )
        .await
        .expect("duplicate event should be fenced");
    assert!(rx.try_recv().is_err());

    let error = mapper
        .forward_envelope(
            &json!({
                "type": "session.event",
                "eventSeq": 6,
                "event": events[3],
            }),
            &tx,
        )
        .await
        .expect_err("sequence gaps must fail closed");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[tokio::test]
async fn live_events_fail_closed_on_out_of_order_or_reopened_turns() {
    let thread_id = deterministic_thread_id("alice").expect("thread id");
    let mut mapper = EventMapper::new(thread_id, 0);
    let (tx, _rx) = mpsc::channel(16);
    let assistant_first = json!({
        "type": "session.event",
        "eventSeq": 1,
        "event": {
            "type": "assistant.message.delta",
            "commandId": "command-1",
            "turnId": "turn-1",
            "messageId": "assistant-1",
            "delta": "nope",
        },
    });
    assert_eq!(
        mapper
            .forward_envelope(&assistant_first, &tx)
            .await
            .expect_err("assistant-first streams must fail closed")
            .kind(),
        ErrorKind::InvalidData
    );
}
