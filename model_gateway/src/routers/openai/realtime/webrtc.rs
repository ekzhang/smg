//! WebRTC signaling handlers for `/v1/realtime/calls`.
//!
//! SMG acts as a WebRTC relay: it terminates the client's peer connection,
//! establishes its own peer connection to upstream, and bridges data-channel
//! messages plus audio RTP packets between the two.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use tracing::{debug, error, info};

use super::{rest::select_worker, webrtc_bridge::WebRtcBridge};
use crate::{
    core::worker::WorkerLoadGuard,
    routers::{error, header_utils::extract_auth_header},
    server::AppState,
};

/// Default STUN server for ICE server-reflexive candidate gathering.
const DEFAULT_STUN_SERVER: &str = "stun.l.google.com:19302";

/// Resolve the default STUN server hostname to an IPv4 `SocketAddr`.
/// Filters for IPv4 since our UDP sockets bind to `0.0.0.0`.
async fn resolve_stun_server() -> Option<SocketAddr> {
    match tokio::net::lookup_host(DEFAULT_STUN_SERVER).await {
        Ok(mut addrs) => addrs.find(|a| a.is_ipv4()),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to resolve STUN server");
            None
        }
    }
}

/// `POST /v1/realtime/calls` — WebRTC SDP signaling.
///
/// Supports two content types:
/// - `multipart/form-data`: Unified interface. Contains `sdp` (SDP offer) and
///   `session` (JSON session config) fields. SMG authenticates with upstream
///   using its own API key.
/// - `application/sdp`: Direct SDP flow. Body is the raw SDP offer.
///   SMG authenticates with upstream using the worker API key.
pub async fn create_call(
    State(state): State<Arc<AppState>>,
    Query(params): Query<super::ws::RealtimeQueryParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if content_type.starts_with("multipart/form-data") {
        create_call_multipart(state, headers, body, &content_type).await
    } else if content_type.starts_with("application/sdp") {
        let model = params.model.as_deref().unwrap_or("");
        create_call_sdp(state, headers, body, model).await
    } else {
        error!(
            content_type,
            "Unsupported Content-Type for /v1/realtime/calls"
        );
        error::bad_request(
            "invalid_content_type",
            "Expected Content-Type: multipart/form-data or application/sdp",
        )
    }
}

/// Unified interface: `multipart/form-data` with `sdp` + `session` fields.
///
/// SMG extracts the model from the session config, selects a worker, creates
/// a dual peer-connection bridge, and returns its own SDP answer to the client.
async fn create_call_multipart(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    content_type: &str,
) -> Response {
    // -- Parse multipart fields ---------------------------------------------
    let boundary = match multer::parse_boundary(content_type) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "Failed to parse multipart boundary");
            return error::bad_request(
                "invalid_multipart",
                "Missing or invalid multipart boundary",
            );
        }
    };

    let mut multipart = multer::Multipart::new(
        futures::stream::once(async move { Ok::<_, std::io::Error>(body) }),
        boundary,
    );

    let mut sdp_offer: Option<Vec<u8>> = None;
    let mut session_json: Option<serde_json::Value> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("sdp") => {
                sdp_offer = field.bytes().await.ok().map(|b| b.to_vec());
            }
            Some("session") => match field.text().await {
                Ok(text) => match serde_json::from_str(&text) {
                    Ok(parsed) => session_json = Some(parsed),
                    Err(e) => {
                        return error::bad_request(
                            "invalid_session_json",
                            format!("Invalid JSON in 'session' field: {e}"),
                        );
                    }
                },
                Err(e) => {
                    return error::bad_request(
                        "unreadable_session",
                        format!("Failed to read 'session' field: {e}"),
                    );
                }
            },
            _ => {}
        }
    }

    let Some(sdp_bytes) = sdp_offer else {
        return error::bad_request("missing_sdp", "multipart 'sdp' field is required");
    };

    let Ok(sdp_str) = String::from_utf8(sdp_bytes) else {
        return error::bad_request("invalid_sdp", "SDP is not valid UTF-8");
    };

    // -- Worker selection ---------------------------------------------------
    let model = session_json
        .as_ref()
        .and_then(|s| s.get("model"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if model.is_empty() {
        return error::bad_request("missing_model", "session.model is required");
    }

    setup_and_spawn_bridge(
        &state,
        &headers,
        &sdp_str,
        &model,
        session_json,
        "multipart",
    )
    .await
}

/// Direct SDP flow: `application/sdp` body is the raw SDP offer.
///
/// SMG routes to the correct upstream worker based on the `model` query
/// parameter. A dual peer-connection bridge is created and the client
/// receives SMG's SDP answer.
async fn create_call_sdp(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    model: &str,
) -> Response {
    if model.is_empty() {
        return error::bad_request(
            "missing_model",
            "query parameter 'model' is required for application/sdp requests",
        );
    }

    let Ok(sdp_str) = std::str::from_utf8(&body) else {
        return error::bad_request("invalid_sdp", "SDP is not valid UTF-8");
    };

    setup_and_spawn_bridge(&state, &headers, sdp_str, model, None, "direct SDP").await
}

// ---------------------------------------------------------------------------
// Shared bridge setup
// ---------------------------------------------------------------------------

/// Worker selection → bridge creation → spawn relay task → return SDP answer.
///
/// Shared by both multipart and direct SDP paths.
async fn setup_and_spawn_bridge(
    state: &AppState,
    headers: &HeaderMap,
    sdp_str: &str,
    model: &str,
    session_config: Option<serde_json::Value>,
    label: &str,
) -> Response {
    let Some(worker) = select_worker(state, model) else {
        error!(model, "No available worker for realtime model");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let Some(auth) = extract_auth_header(Some(headers), worker.api_key()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let auth_str = auth.to_str().unwrap_or("").to_string();

    let _guard = WorkerLoadGuard::new(worker.clone(), Some(headers));

    let upstream_url = format!(
        "{}/v1/realtime/calls?model={model}",
        worker.url().trim_end_matches('/')
    );

    let call_id = uuid::Uuid::now_v7().to_string();
    let stun_server = resolve_stun_server().await;

    info!(
        call_id,
        model,
        upstream_url,
        ?stun_server,
        "Creating WebRTC bridge ({label})"
    );

    let bind_addr = state
        .context
        .webrtc_bind_addr
        .unwrap_or_else(|| std::net::Ipv4Addr::UNSPECIFIED.into());

    let (mut bridge, client_sdp_answer) = match WebRtcBridge::setup(
        sdp_str,
        &upstream_url,
        &auth_str,
        session_config,
        call_id.clone(),
        &state.context.client,
        bind_addr,
        stun_server,
    )
    .await
    {
        Ok(result) => {
            worker.record_outcome(true);
            result
        }
        Err(e) => {
            error!(call_id, model, error = %e, "Failed to create WebRTC bridge ({label})");
            worker.record_outcome(false);
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    // -- Register call and spawn bridge task --------------------------------
    let registry = Arc::clone(&state.context.realtime_registry);
    let entry =
        registry.register_call(call_id.clone(), model.to_string(), worker.url().to_string());
    // Use the registry's cancel token so hangup cancellation reaches the bridge.
    bridge.set_cancel_token(entry.cancel_token);

    let bridge_registry = Arc::clone(&registry);
    let bridge_call_id = call_id.clone();
    #[expect(
        clippy::disallowed_methods,
        reason = "bridge task self-terminates on disconnect/cancel"
    )]
    tokio::spawn(async move {
        Box::pin(bridge.run(bridge_registry.clone())).await;
        bridge_registry.remove_call(&bridge_call_id);
        debug!(call_id = bridge_call_id, "WebRTC bridge task completed");
    });

    debug!(call_id, model, "WebRTC bridge started ({label})");

    // -- Return SMG-generated SDP answer ------------------------------------
    #[expect(
        clippy::expect_used,
        reason = "infallible: static header names and valid body"
    )]
    Response::builder()
        .status(StatusCode::CREATED)
        .header("Content-Type", "application/sdp")
        .body(axum::body::Body::from(client_sdp_answer))
        .expect("static response builder")
}

/// `POST /v1/realtime/calls/{call_id}/hangup` — Terminate a WebRTC call.
///
/// Cancels the bridge relay task, which disconnects both the client-facing
/// and upstream peer connections via ICE teardown.  No upstream HTTP
/// forwarding — OpenAI does not expose a call ID in the direct WebRTC
/// flow, so the upstream connection is terminated by the ICE disconnect.
pub async fn hangup_call(
    State(state): State<Arc<AppState>>,
    Path(call_id): Path<String>,
) -> Response {
    let registry = Arc::clone(&state.context.realtime_registry);

    match registry.remove_call(&call_id) {
        Some(entry) => {
            entry.cancel_token.cancel();
            info!(call_id, "WebRTC call hung up");
            StatusCode::OK.into_response()
        }
        None => {
            debug!(call_id, "Hangup requested for unknown call");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}
