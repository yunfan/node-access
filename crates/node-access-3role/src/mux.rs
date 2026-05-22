use crate::crypto::FrameCrypto;
use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, BytesMut};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const FRAME_OPEN: u8 = 1;
const FRAME_DATA: u8 = 2;
const FRAME_CLOSE: u8 = 3;
const FRAME_CONTROL: u8 = 4;
const FRAME_ENCRYPTED: u8 = 0xE1;

pub type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsRead = SplitStream<WsStream>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OpenRequest {
    Socks,
    Visit {
        name: String,
        #[serde(default)]
        auth: Option<String>,
    },
    ProviderConnect {
        name: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    Bind {
        name: String,
        #[serde(default)]
        auth: Option<String>,
    },
    BindAck {
        name: String,
        ok: bool,
        error: Option<String>,
    },
    Info {
        message: String,
    },
}

#[derive(Debug)]
enum MuxFrame {
    Open {
        stream_id: u32,
        request: OpenRequest,
    },
    Data {
        stream_id: u32,
        payload: Vec<u8>,
    },
    Close {
        stream_id: u32,
    },
    Control(ControlMessage),
}

pub struct IncomingStream {
    pub request: OpenRequest,
    stream: VirtualStream,
}

impl IncomingStream {
    pub fn into_stream(self) -> VirtualStream {
        self.stream
    }
}

#[derive(Clone)]
pub struct MuxHandle {
    outbound: mpsc::UnboundedSender<MuxFrame>,
    streams: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<Vec<u8>>>>>,
    next_stream_id: Arc<AtomicU32>,
}

pub struct MuxConnection {
    pub handle: MuxHandle,
    pub incoming: mpsc::UnboundedReceiver<IncomingStream>,
    pub control: mpsc::UnboundedReceiver<ControlMessage>,
    pub closed: mpsc::UnboundedReceiver<()>,
}

impl MuxConnection {
    pub fn start(ws: WsStream, first_stream_id: u32, crypto: Option<FrameCrypto>) -> Self {
        let (sink, read) = ws.split();
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (closed_tx, closed_rx) = mpsc::unbounded_channel();
        let streams = Arc::new(Mutex::new(HashMap::new()));
        let handle = MuxHandle {
            outbound: outbound_tx,
            streams: Arc::clone(&streams),
            next_stream_id: Arc::new(AtomicU32::new(first_stream_id)),
        };

        tokio::spawn(write_loop(sink, outbound_rx, crypto.clone()));
        tokio::spawn(read_loop(
            read,
            crypto,
            streams,
            handle.outbound.clone(),
            incoming_tx,
            control_tx,
            closed_tx,
        ));

        Self {
            handle,
            incoming: incoming_rx,
            control: control_rx,
            closed: closed_rx,
        }
    }

    pub async fn closed(&mut self) {
        let _ = self.closed.recv().await;
    }
}

impl MuxHandle {
    pub async fn open_stream(&self, request: OpenRequest) -> Result<VirtualStream> {
        let stream_id = self.next_stream_id.fetch_add(2, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        self.streams.lock().await.insert(stream_id, tx);
        self.outbound
            .send(MuxFrame::Open { stream_id, request })
            .map_err(|_| anyhow!("mux writer is closed"))?;
        Ok(VirtualStream::new(
            stream_id,
            rx,
            self.outbound.clone(),
            Arc::clone(&self.streams),
        ))
    }

    pub fn send_control(&self, message: ControlMessage) -> Result<()> {
        self.outbound
            .send(MuxFrame::Control(message))
            .map_err(|_| anyhow!("mux writer is closed"))
    }
}

pub struct VirtualStream {
    stream_id: u32,
    inbound: mpsc::UnboundedReceiver<Vec<u8>>,
    read_buffer: VecDeque<u8>,
    outbound: mpsc::UnboundedSender<MuxFrame>,
    streams: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<Vec<u8>>>>>,
    sent_close: bool,
}

impl VirtualStream {
    fn new(
        stream_id: u32,
        inbound: mpsc::UnboundedReceiver<Vec<u8>>,
        outbound: mpsc::UnboundedSender<MuxFrame>,
        streams: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<Vec<u8>>>>>,
    ) -> Self {
        Self {
            stream_id,
            inbound,
            read_buffer: VecDeque::new(),
            outbound,
            streams,
            sent_close: false,
        }
    }

    pub fn close_remote(&mut self) {
        if self.sent_close {
            return;
        }
        self.sent_close = true;
        let _ = self.outbound.send(MuxFrame::Close {
            stream_id: self.stream_id,
        });
    }
}

impl Drop for VirtualStream {
    fn drop(&mut self) {
        self.close_remote();
        let streams = Arc::clone(&self.streams);
        let stream_id = self.stream_id;
        tokio::spawn(async move {
            streams.lock().await.remove(&stream_id);
        });
    }
}

impl AsyncRead for VirtualStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        while buf.remaining() > 0 {
            match self.read_buffer.pop_front() {
                Some(byte) => buf.put_slice(&[byte]),
                None => break,
            }
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        match Pin::new(&mut self.inbound).poll_recv(cx) {
            Poll::Ready(Some(bytes)) => {
                let to_copy = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..to_copy]);
                self.read_buffer.extend(bytes[to_copy..].iter().copied());
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => {
                if buf.filled().is_empty() {
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}

impl AsyncWrite for VirtualStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.outbound
            .send(MuxFrame::Data {
                stream_id: self.stream_id,
                payload: data.to_vec(),
            })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "mux closed"))?;
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.close_remote();
        Poll::Ready(Ok(()))
    }
}

async fn write_loop(
    mut sink: WsSink,
    mut outbound: mpsc::UnboundedReceiver<MuxFrame>,
    crypto: Option<FrameCrypto>,
) {
    while let Some(frame) = outbound.recv().await {
        let mut bytes = match encode_frame(frame) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(%error, "failed to encode mux frame");
                continue;
            }
        };
        if let Some(crypto) = &crypto {
            match crypto.seal(&bytes) {
                Ok((counter, ciphertext)) => {
                    let mut wrapped = Vec::with_capacity(1 + 8 + ciphertext.len());
                    wrapped.push(FRAME_ENCRYPTED);
                    wrapped.extend_from_slice(&counter.to_be_bytes());
                    wrapped.extend_from_slice(&ciphertext);
                    bytes = wrapped;
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to encrypt mux frame");
                    break;
                }
            }
        }
        if sink.send(Message::Binary(bytes)).await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
}

async fn read_loop(
    mut read: WsRead,
    crypto: Option<FrameCrypto>,
    streams: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<Vec<u8>>>>>,
    outbound: mpsc::UnboundedSender<MuxFrame>,
    incoming: mpsc::UnboundedSender<IncomingStream>,
    control: mpsc::UnboundedSender<ControlMessage>,
    closed: mpsc::UnboundedSender<()>,
) {
    while let Some(message) = read.next().await {
        let Message::Binary(mut bytes) = (match message {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(%error, "relay websocket read failed");
                break;
            }
        }) else {
            continue;
        };

        if let Some(crypto) = &crypto {
            if bytes.first().copied() != Some(FRAME_ENCRYPTED) || bytes.len() < 9 {
                tracing::warn!("received unencrypted frame while --secret is configured");
                break;
            }
            let mut counter_bytes = [0u8; 8];
            counter_bytes.copy_from_slice(&bytes[1..9]);
            let counter = u64::from_be_bytes(counter_bytes);
            match crypto.open(counter, &bytes[9..]) {
                Ok(plaintext) => bytes = plaintext,
                Err(error) => {
                    tracing::warn!(%error, "failed to decrypt mux frame");
                    break;
                }
            }
        }

        match decode_frame(&bytes) {
            Ok(MuxFrame::Open { stream_id, request }) => {
                let (tx, rx) = mpsc::unbounded_channel();
                streams.lock().await.insert(stream_id, tx);
                let stream =
                    VirtualStream::new(stream_id, rx, outbound.clone(), Arc::clone(&streams));
                let _ = incoming.send(IncomingStream { request, stream });
            }
            Ok(MuxFrame::Data { stream_id, payload }) => {
                let tx = streams.lock().await.get(&stream_id).cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(payload);
                }
            }
            Ok(MuxFrame::Close { stream_id }) => {
                streams.lock().await.remove(&stream_id);
            }
            Ok(MuxFrame::Control(message)) => {
                let _ = control.send(message);
            }
            Err(error) => {
                tracing::warn!(%error, "invalid mux frame");
                break;
            }
        }
    }
    let _ = closed.send(());
}

fn encode_frame(frame: MuxFrame) -> Result<Vec<u8>> {
    let mut out = BytesMut::new();
    match frame {
        MuxFrame::Open { stream_id, request } => {
            out.put_u8(FRAME_OPEN);
            out.put_u32(stream_id);
            out.extend_from_slice(&serde_json::to_vec(&request)?);
        }
        MuxFrame::Data { stream_id, payload } => {
            out.put_u8(FRAME_DATA);
            out.put_u32(stream_id);
            out.extend_from_slice(&payload);
        }
        MuxFrame::Close { stream_id } => {
            out.put_u8(FRAME_CLOSE);
            out.put_u32(stream_id);
        }
        MuxFrame::Control(message) => {
            out.put_u8(FRAME_CONTROL);
            out.put_u32(0);
            out.extend_from_slice(&serde_json::to_vec(&message)?);
        }
    }
    Ok(out.to_vec())
}

fn decode_frame(bytes: &[u8]) -> Result<MuxFrame> {
    if bytes.len() < 5 {
        return Err(anyhow!("frame too short"));
    }
    let kind = bytes[0];
    let stream_id = u32::from_be_bytes(bytes[1..5].try_into().expect("slice length checked"));
    let payload = &bytes[5..];
    match kind {
        FRAME_OPEN => Ok(MuxFrame::Open {
            stream_id,
            request: serde_json::from_slice(payload).context("invalid open frame")?,
        }),
        FRAME_DATA => Ok(MuxFrame::Data {
            stream_id,
            payload: payload.to_vec(),
        }),
        FRAME_CLOSE => Ok(MuxFrame::Close { stream_id }),
        FRAME_CONTROL => Ok(MuxFrame::Control(
            serde_json::from_slice(payload).context("invalid control frame")?,
        )),
        _ => Err(anyhow!("unknown frame type {kind}")),
    }
}
