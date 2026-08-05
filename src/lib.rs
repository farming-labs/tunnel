use std::{
    collections::HashMap,
    sync::{Mutex as StdMutex, OnceLock},
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use napi::{Error, Result, Status};
use napi_derive::napi;
use reqwest::{Client, Method, Url, header::HeaderMap};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::{Mutex as AsyncMutex, oneshot, watch},
    time::{MissedTickBehavior, interval, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type AgentSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

struct AgentControl {
    stop: oneshot::Sender<()>,
    done: watch::Receiver<bool>,
}

static AGENTS: OnceLock<StdMutex<HashMap<String, AgentControl>>> = OnceLock::new();

fn agents() -> &'static StdMutex<HashMap<String, AgentControl>> {
    AGENTS.get_or_init(|| StdMutex::new(HashMap::new()))
}

#[napi(object)]
pub struct PreviewAgentSession {
    pub session_id: String,
    pub public_url: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum AgentMessage {
    Register {
        name: String,
    },
    Response {
        id: String,
        status: u16,
        headers: HashMap<String, String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        body: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum RelayMessage {
    #[serde(rename_all = "camelCase")]
    Ready {
        session_id: String,
        public_url: String,
    },
    Request {
        id: String,
        method: String,
        path: String,
        headers: HashMap<String, String>,
        body: Option<String>,
    },
    Error {
        message: String,
    },
}

#[napi(js_name = "startPreviewAgent")]
pub async fn start_preview_agent(
    relay_url: String,
    name: String,
    target_url: String,
) -> Result<PreviewAgentSession> {
    let target = Url::parse(&target_url)
        .map_err(|error| napi_error(format!("Invalid local target URL: {error}")))?;
    let (websocket, _) = timeout(Duration::from_secs(10), connect_async(&relay_url))
        .await
        .map_err(|_| napi_error("Timed out connecting to the persistent preview relay."))?
        .map_err(|error| napi_error(format!("Could not connect to preview relay: {error}")))?;
    let (mut sink, mut stream) = websocket.split();

    send_message(&mut sink, &AgentMessage::Register { name: name.clone() }).await?;

    let ready = timeout(Duration::from_secs(10), stream.next())
        .await
        .map_err(|_| napi_error("Preview relay did not register the Rust agent in time."))?
        .ok_or_else(|| napi_error("Preview relay closed before registration completed."))?
        .map_err(|error| napi_error(format!("Preview relay registration failed: {error}")))?;
    let ready: RelayMessage = parse_message(ready)?;
    let (session_id, public_url) = match ready {
        RelayMessage::Ready {
            session_id,
            public_url,
        } => (session_id, public_url),
        RelayMessage::Error { message } => return Err(napi_error(message)),
        _ => {
            return Err(napi_error(
                "Preview relay returned an unexpected registration message.",
            ));
        }
    };

    let sink = std::sync::Arc::new(AsyncMutex::new(sink));
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let (done_tx, done_rx) = watch::channel(false);
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| napi_error(format!("Could not create the Rust HTTP client: {error}")))?;
    let task_sink = sink.clone();

    napi::tokio::spawn(async move {
        let mut local_probe = interval(Duration::from_secs(2));
        local_probe.set_missed_tick_behavior(MissedTickBehavior::Delay);
        local_probe.tick().await;

        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    let _ = task_sink.lock().await.send(Message::Close(None)).await;
                    break;
                }
                incoming = stream.next() => {
                    let Some(incoming) = incoming else { break };
                    let Ok(incoming) = incoming else { break };
                    let Ok(message) = parse_relay_message(incoming) else { continue };
                    if let RelayMessage::Request { id, method, path, headers, body } = message {
                        let request = TunnelRequest { id, method, path, headers, body };
                        let request_client = client.clone();
                        let request_target = target.clone();
                        let request_sink = task_sink.clone();
                        napi::tokio::spawn(async move {
                            let response = forward_request(&request_client, &request_target, request).await;
                            let payload = serde_json::to_string(&response);
                            if let Ok(payload) = payload {
                                let _ = request_sink.lock().await.send(Message::Text(payload.into())).await;
                            }
                        });
                    }
                }
                _ = local_probe.tick() => {
                    let reachable = client
                        .get(target.clone())
                        .timeout(Duration::from_secs(1))
                        .send()
                        .await
                        .is_ok();
                    if !reachable {
                        let _ = task_sink.lock().await.send(Message::Close(None)).await;
                        break;
                    }
                }
            }
        }
        let _ = done_tx.send(true);
    });

    agents()
        .lock()
        .map_err(|_| napi_error("Rust preview agent registry is unavailable."))?
        .insert(
            session_id.clone(),
            AgentControl {
                stop: stop_tx,
                done: done_rx,
            },
        );

    Ok(PreviewAgentSession {
        session_id,
        public_url,
    })
}

#[napi(js_name = "stopPreviewAgent")]
pub async fn stop_preview_agent(session_id: String) -> Result<bool> {
    let control = agents()
        .lock()
        .map_err(|_| napi_error("Rust preview agent registry is unavailable."))?
        .remove(&session_id);
    let Some(control) = control else {
        return Ok(false);
    };

    let mut done = control.done;
    let _ = control.stop.send(());
    if !*done.borrow() {
        let _ = timeout(Duration::from_secs(5), done.changed()).await;
    }
    Ok(true)
}

#[napi(js_name = "waitPreviewAgent")]
pub async fn wait_preview_agent(session_id: String) -> Result<bool> {
    let mut done = agents()
        .lock()
        .map_err(|_| napi_error("Rust preview agent registry is unavailable."))?
        .get(&session_id)
        .map(|control| control.done.clone());
    let Some(ref mut done) = done else {
        return Ok(false);
    };

    if !*done.borrow() {
        let _ = done.changed().await;
    }

    agents()
        .lock()
        .map_err(|_| napi_error("Rust preview agent registry is unavailable."))?
        .remove(&session_id);
    Ok(true)
}

#[napi(js_name = "activePreviewAgentCount")]
pub fn active_preview_agent_count() -> Result<u32> {
    let count = agents()
        .lock()
        .map_err(|_| napi_error("Rust preview agent registry is unavailable."))?
        .values()
        .filter(|control| !*control.done.borrow())
        .count();
    Ok(count as u32)
}

struct TunnelRequest {
    id: String,
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Option<String>,
}

async fn forward_request(client: &Client, target: &Url, request: TunnelRequest) -> AgentMessage {
    match try_forward_request(client, target, &request).await {
        Ok((status, headers, body)) => AgentMessage::Response {
            id: request.id,
            status,
            headers,
            body: Some(BASE64.encode(body)),
        },
        Err(error) => AgentMessage::Response {
            id: request.id,
            status: 502,
            headers: HashMap::from([(
                "content-type".to_string(),
                "text/plain; charset=utf-8".to_string(),
            )]),
            body: Some(BASE64.encode(error.to_string())),
        },
    }
}

async fn try_forward_request(
    client: &Client,
    target: &Url,
    request: &TunnelRequest,
) -> std::result::Result<
    (u16, HashMap<String, String>, Vec<u8>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let url = target.join(request.path.trim_start_matches('/'))?;
    let method = Method::from_bytes(request.method.as_bytes())?;
    let mut builder = client.request(method.clone(), url);
    let mut headers = HeaderMap::new();
    for (name, value) in &request.headers {
        if is_hop_by_hop_header(name) || name.eq_ignore_ascii_case("host") {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_bytes()),
            reqwest::header::HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
    builder = builder.headers(headers);

    if method != Method::GET
        && method != Method::HEAD
        && let Some(body) = &request.body
    {
        builder = builder.body(BASE64.decode(body)?);
    }

    let response = builder.send().await?;
    let status = response.status().as_u16();
    let response_headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            if is_hop_by_hop_header(name.as_str()) {
                return None;
            }
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect();
    let body = response.bytes().await?.to_vec();
    Ok((status, response_headers, body))
}

fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "content-length"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

async fn send_message(sink: &mut AgentSink, message: &AgentMessage) -> Result<()> {
    let payload = serde_json::to_string(message)
        .map_err(|error| napi_error(format!("Could not encode tunnel message: {error}")))?;
    sink.send(Message::Text(payload.into()))
        .await
        .map_err(|error| napi_error(format!("Could not send tunnel message: {error}")))
}

fn parse_message(message: Message) -> Result<RelayMessage> {
    parse_relay_message(message).map_err(napi_error)
}

fn parse_relay_message(message: Message) -> std::result::Result<RelayMessage, String> {
    let text = message
        .into_text()
        .map_err(|error| format!("Tunnel message was not text: {error}"))?;
    serde_json::from_str(&text).map_err(|error| format!("Could not decode tunnel message: {error}"))
}

fn napi_error(message: impl Into<String>) -> Error {
    Error::new(Status::GenericFailure, message.into())
}
