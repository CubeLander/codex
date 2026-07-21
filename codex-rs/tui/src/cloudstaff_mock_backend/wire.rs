use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::UserInput;
use serde_json::Value;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

pub(super) async fn write_json_line(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    value: &Value,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(invalid_wire)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await
}

pub(super) async fn read_json_line(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
) -> Result<Option<Value>> {
    lines
        .next_line()
        .await?
        .map(|line| serde_json::from_str(&line).map_err(invalid_wire))
        .transpose()
}

pub(super) fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_data(format!("mock message omitted {field}")))
}

pub(super) fn required_u64(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_data(format!("mock message omitted {field}")))
}

pub(super) fn wire_error(value: &Value) -> Error {
    let code = value
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("protocol_error");
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("mock rejected request");
    Error::new(ErrorKind::InvalidData, format!("{code}: {message}"))
}

pub(super) fn request_method_name(request: &ClientRequest) -> String {
    serde_json::to_value(request)
        .ok()
        .and_then(|value| value.get("method")?.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "<unknown>".to_string())
}

pub(super) fn text_only_input(input: Vec<UserInput>) -> Result<String> {
    let mut text = String::new();
    for item in input {
        match item {
            UserInput::Text { text: part, .. } => text.push_str(&part),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "CloudStaff mock accepts text-only turns",
                ));
            }
        }
    }
    if text.is_empty() {
        return Err(invalid_data("CloudStaff mock turn text must not be empty"));
    }
    Ok(text)
}

pub(super) fn invalid_wire(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::InvalidData, error.to_string())
}

pub(super) fn invalid_data(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidData, message.into())
}
