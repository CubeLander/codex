//! TUI-local backend facade.
//!
//! The TUI currently uses app-server directly. Keeping that implementation
//! behind this small enum leaves a mechanics seam for a future CloudStaff
//! transport without changing renderer, core, or app-server contracts.

use codex_app_server_client::AppServerClient;
use codex_app_server_client::AppServerEvent;
use codex_app_server_client::AppServerPath;
use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_client::TypedRequestError;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Result as JsonRpcResult;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::de::DeserializeOwned;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
#[cfg(unix)]
use std::path::Path;

#[cfg(unix)]
use crate::cloudstaff_mock_backend::CloudStaffBackendClient;
#[cfg(unix)]
use crate::cloudstaff_mock_backend::CloudStaffBackendRequestHandle;

const CLOUDSTAFF_BACKEND_UNAVAILABLE: &str =
    "CloudStaff session backend is not connected in this build";

pub(crate) enum TuiBackendClient {
    AppServer(AppServerClient),
    #[cfg(unix)]
    CloudStaff(CloudStaffBackendClient),
}

#[derive(Clone)]
pub(crate) enum TuiBackendRequestHandle {
    AppServer(AppServerRequestHandle),
    #[cfg(unix)]
    CloudStaff(CloudStaffBackendRequestHandle),
}

impl TuiBackendClient {
    pub(crate) fn app_server(client: AppServerClient) -> Self {
        Self::AppServer(client)
    }

    #[cfg(unix)]
    pub(crate) async fn connect_cloudstaff_mock(
        socket_path: &Path,
        session_id: String,
        device_id: String,
    ) -> Result<Self> {
        CloudStaffBackendClient::connect(socket_path, session_id, device_id)
            .await
            .map(Self::CloudStaff)
    }

    pub(crate) fn uses_embedded_app_server(&self) -> bool {
        matches!(self, Self::AppServer(AppServerClient::InProcess(_)))
    }

    pub(crate) fn codex_home(&self, local_codex_home: &AbsolutePathBuf) -> Option<AppServerPath> {
        match self {
            Self::AppServer(client) => client.codex_home(local_codex_home),
            #[cfg(unix)]
            Self::CloudStaff(_) => None,
        }
    }

    pub(crate) fn server_version(&self) -> Option<&str> {
        match self {
            Self::AppServer(AppServerClient::Remote(client)) => client.server_version(),
            Self::AppServer(AppServerClient::InProcess(_)) => None,
            #[cfg(unix)]
            Self::CloudStaff(_) => None,
        }
    }

    pub(crate) async fn request_typed<T>(
        &self,
        request: ClientRequest,
    ) -> std::result::Result<T, TypedRequestError>
    where
        T: DeserializeOwned,
    {
        match self {
            Self::AppServer(client) => client.request_typed(request).await,
            #[cfg(unix)]
            Self::CloudStaff(client) => {
                let method = request_method_name(&request);
                let response =
                    client
                        .request_handle()
                        .request(request)
                        .await
                        .map_err(|source| TypedRequestError::Transport {
                            method: method.clone(),
                            source,
                        })?;
                serde_json::from_value(response)
                    .map_err(|source| TypedRequestError::Deserialize { method, source })
            }
        }
    }

    pub(crate) async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: JsonRpcResult,
    ) -> Result<()> {
        match self {
            Self::AppServer(client) => client.resolve_server_request(request_id, result).await,
            #[cfg(unix)]
            Self::CloudStaff(_) => Err(unavailable_error()),
        }
    }

    pub(crate) async fn reject_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> Result<()> {
        match self {
            Self::AppServer(client) => client.reject_server_request(request_id, error).await,
            #[cfg(unix)]
            Self::CloudStaff(_) => Err(unavailable_error()),
        }
    }

    pub(crate) async fn next_event(&mut self) -> Option<AppServerEvent> {
        match self {
            Self::AppServer(client) => client.next_event().await,
            #[cfg(unix)]
            Self::CloudStaff(client) => client.next_event().await,
        }
    }

    pub(crate) async fn shutdown(self) -> Result<()> {
        match self {
            Self::AppServer(client) => client.shutdown().await,
            #[cfg(unix)]
            Self::CloudStaff(client) => client.shutdown().await,
        }
    }

    pub(crate) fn request_handle(&self) -> TuiBackendRequestHandle {
        match self {
            Self::AppServer(client) => TuiBackendRequestHandle::AppServer(client.request_handle()),
            #[cfg(unix)]
            Self::CloudStaff(client) => {
                TuiBackendRequestHandle::CloudStaff(client.request_handle())
            }
        }
    }

    pub(crate) fn cloudstaff_attached_response(
        &self,
        model: String,
        cwd: AbsolutePathBuf,
    ) -> Option<codex_app_server_protocol::ThreadResumeResponse> {
        match self {
            Self::AppServer(_) => None,
            #[cfg(unix)]
            Self::CloudStaff(client) => Some(client.attached_response(model, cwd)),
        }
    }

    pub(crate) fn is_cloudstaff(&self) -> bool {
        cfg!(unix) && matches_cloudstaff(self)
    }
}

impl TuiBackendRequestHandle {
    pub(crate) async fn request_typed<T>(
        &self,
        request: ClientRequest,
    ) -> std::result::Result<T, TypedRequestError>
    where
        T: DeserializeOwned,
    {
        match self {
            Self::AppServer(handle) => handle.request_typed(request).await,
            #[cfg(unix)]
            Self::CloudStaff(handle) => {
                let method = request_method_name(&request);
                let response = handle.request(request).await.map_err(|source| {
                    TypedRequestError::Transport {
                        method: method.clone(),
                        source,
                    }
                })?;
                serde_json::from_value(response)
                    .map_err(|source| TypedRequestError::Deserialize { method, source })
            }
        }
    }

    pub(crate) fn uses_embedded_app_server(&self) -> bool {
        matches!(self, Self::AppServer(AppServerRequestHandle::InProcess(_)))
    }
}

#[cfg(unix)]
fn matches_cloudstaff(client: &TuiBackendClient) -> bool {
    matches!(client, TuiBackendClient::CloudStaff(_))
}

#[cfg(not(unix))]
fn matches_cloudstaff(_client: &TuiBackendClient) -> bool {
    false
}

fn unavailable_error() -> Error {
    Error::new(ErrorKind::Unsupported, CLOUDSTAFF_BACKEND_UNAVAILABLE)
}

fn request_method_name(request: &ClientRequest) -> String {
    serde_json::to_value(request)
        .ok()
        .and_then(|value| {
            value
                .get("method")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "<unknown>".to_string())
}
