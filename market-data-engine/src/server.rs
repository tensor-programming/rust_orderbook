//! Concurrent WebSocket server implementation using tokio-tungstenite,
//! including an in-memory mock WebSocket interface for fast, non-blocking testing.

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::broadcaster::MarketDataBroadcaster;
use crate::messages::{ClientMessage, ServerMessage};
use crate::subscription::{Channel, SubscriptionManager, Topic};

/// Configuration for the WebSocket server.
#[derive(Debug, Clone)]
pub struct WsServerConfig {
    pub client_channel_capacity: usize,
}

impl Default for WsServerConfig {
    fn default() -> Self {
        Self {
            client_channel_capacity: 1024,
        }
    }
}

/// The market data WebSocket server handling concurrent client connections.
pub struct MarketDataWsServer {
    broadcaster: Arc<MarketDataBroadcaster>,
    subscriptions: Arc<SubscriptionManager>,
    config: WsServerConfig,
}

impl MarketDataWsServer {
    pub fn new(
        broadcaster: Arc<MarketDataBroadcaster>,
        subscriptions: Arc<SubscriptionManager>,
    ) -> Self {
        Self::with_config(broadcaster, subscriptions, WsServerConfig::default())
    }

    pub fn with_config(
        broadcaster: Arc<MarketDataBroadcaster>,
        subscriptions: Arc<SubscriptionManager>,
        config: WsServerConfig,
    ) -> Self {
        Self {
            broadcaster,
            subscriptions,
            config,
        }
    }

    /// Access the broadcaster to emit events from external producers.
    pub fn broadcaster(&self) -> Arc<MarketDataBroadcaster> {
        Arc::clone(&self.broadcaster)
    }

    /// Access the subscription manager.
    pub fn subscriptions(&self) -> Arc<SubscriptionManager> {
        Arc::clone(&self.subscriptions)
    }

    /// Create an in-memory fake WebSocket client session for fast testing without TCP sockets.
    pub fn create_mock_client(&self) -> MockWsSession {
        MockWsSession::new(
            Arc::clone(&self.broadcaster),
            Arc::clone(&self.subscriptions),
        )
    }

    /// Run the WebSocket server listening on the provided address.
    pub async fn run(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        info!("MarketData WebSocket server listening on ws://{}", addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let broadcaster = Arc::clone(&self.broadcaster);
                    let subscriptions = Arc::clone(&self.subscriptions);
                    let channel_capacity = self.config.client_channel_capacity;

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            stream,
                            peer_addr,
                            broadcaster,
                            subscriptions,
                            channel_capacity,
                        )
                        .await
                        {
                            debug!(%peer_addr, "WebSocket connection finished: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("TCP accept error on WebSocket server: {}", e);
                }
            }
        }
    }

    /// Handle an individual client connection lifecycle.
    async fn handle_connection(
        stream: TcpStream,
        peer_addr: SocketAddr,
        broadcaster: Arc<MarketDataBroadcaster>,
        subscriptions: Arc<SubscriptionManager>,
        channel_capacity: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ws_stream = tokio_tungstenite::accept_async(stream).await?;
        let client_id = Uuid::new_v4();
        info!(%client_id, %peer_addr, "WebSocket client connected");

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<ServerMessage>(channel_capacity);

        // Register client in broadcaster & subscriptions
        broadcaster.register_client(client_id, outbound_tx.clone());

        // Send initial connection greeting (server.js compatible format)
        let welcome = ServerMessage::Connected {
            message: "Connected to Market Data WebSocket server".to_string(),
        };
        let welcome_json = serde_json::to_string(&welcome)?;
        ws_sender.send(Message::Text(welcome_json.into())).await?;

        // Spawn writer task
        let writer_handle = tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                match serde_json::to_string(&msg) {
                    Ok(json) => {
                        if let Err(e) = ws_sender.send(Message::Text(json.into())).await {
                            debug!("Error sending WebSocket frame: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize server message: {}", e);
                    }
                }
            }
        });

        // Reader loop
        while let Some(msg_result) = ws_receiver.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    Self::process_client_message(
                        &text,
                        &client_id,
                        &broadcaster,
                        &subscriptions,
                        &outbound_tx,
                    )
                    .await;
                }
                Ok(Message::Ping(_)) => {
                    let pong = ServerMessage::Pong {
                        timestamp: Utc::now().timestamp_millis(),
                    };
                    let _ = outbound_tx.send(pong).await;
                }
                Ok(Message::Close(_)) => {
                    debug!(%client_id, "Client sent Close frame");
                    break;
                }
                Err(e) => {
                    warn!(%client_id, "WebSocket read error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        // Cleanup on disconnect
        broadcaster.unregister_client(&client_id);
        writer_handle.abort();
        info!(%client_id, %peer_addr, "WebSocket client disconnected");

        Ok(())
    }

    /// Process incoming JSON message from a connected client.
    pub async fn process_client_message(
        text: &str,
        client_id: &Uuid,
        _broadcaster: &Arc<MarketDataBroadcaster>,
        subscriptions: &Arc<SubscriptionManager>,
        outbound_tx: &mpsc::Sender<ServerMessage>,
    ) {
        match serde_json::from_str::<ClientMessage>(text) {
            Ok(ClientMessage::Subscribe {
                market_id,
                symbol,
                channel,
                timeframe,
                ..
            }) => {
                let chan = channel
                    .as_deref()
                    .map(|c| Channel::parse(c, timeframe.as_deref()))
                    .unwrap_or(Channel::All);
                let target = market_id.as_deref().or(symbol.as_deref()).unwrap_or("*");

                let topic = Topic::new(target, chan);
                subscriptions.subscribe(client_id, topic);

                info!(%client_id, %target, channel = chan.as_str(), "Client subscribed");

                let ack = ServerMessage::Subscribed {
                    market_id,
                    symbol,
                    channel: chan.as_str().to_string(),
                    timeframe,
                };
                let _ = outbound_tx.send(ack).await;
            }
            Ok(ClientMessage::Unsubscribe {
                market_id,
                symbol,
                channel,
                timeframe,
                ..
            }) => {
                let chan = channel
                    .as_deref()
                    .map(|c| Channel::parse(c, timeframe.as_deref()))
                    .unwrap_or(Channel::All);
                let target = market_id.as_deref().or(symbol.as_deref()).unwrap_or("*");

                let topic = Topic::new(target, chan);
                subscriptions.unsubscribe(client_id, &topic);

                info!(%client_id, %target, channel = chan.as_str(), "Client unsubscribed");

                let ack = ServerMessage::Unsubscribed {
                    market_id,
                    symbol,
                    channel: chan.as_str().to_string(),
                    timeframe,
                };
                let _ = outbound_tx.send(ack).await;
            }
            Ok(ClientMessage::Ping) => {
                let pong = ServerMessage::Pong {
                    timestamp: Utc::now().timestamp_millis(),
                };
                let _ = outbound_tx.send(pong).await;
            }
            Err(e) => {
                warn!(%client_id, "Invalid client message JSON: {}", e);
                let err_msg = ServerMessage::Error {
                    message: format!("Invalid message format: {}", e),
                };
                let _ = outbound_tx.send(err_msg).await;
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Mock WebSocket Client Interface for In-Memory Testing
// ═══════════════════════════════════════════════════════════════════

/// An in-memory fake WebSocket client session for fast, deterministic unit and integration testing.
pub struct MockWsSession {
    pub client_id: Uuid,
    broadcaster: Arc<MarketDataBroadcaster>,
    subscriptions: Arc<SubscriptionManager>,
    outbound_rx: mpsc::Receiver<ServerMessage>,
    outbound_tx: mpsc::Sender<ServerMessage>,
}

impl MockWsSession {
    pub fn new(
        broadcaster: Arc<MarketDataBroadcaster>,
        subscriptions: Arc<SubscriptionManager>,
    ) -> Self {
        let client_id = Uuid::new_v4();
        let (outbound_tx, outbound_rx) = mpsc::channel(1024);
        broadcaster.register_client(client_id, outbound_tx.clone());

        // Send initial connected greeting
        let welcome = ServerMessage::Connected {
            message: "Connected to Market Data WebSocket server".to_string(),
        };
        let _ = outbound_tx.try_send(welcome);

        Self {
            client_id,
            broadcaster,
            subscriptions,
            outbound_rx,
            outbound_tx,
        }
    }

    /// Simulate sending a raw JSON message string from the client.
    pub async fn send_text(&self, text: &str) {
        MarketDataWsServer::process_client_message(
            text,
            &self.client_id,
            &self.broadcaster,
            &self.subscriptions,
            &self.outbound_tx,
        )
        .await;
    }

    /// Simulate sending a structured `ClientMessage`.
    pub async fn send_message(&self, msg: ClientMessage) {
        let json = serde_json::to_string(&msg).unwrap();
        self.send_text(&json).await;
    }

    /// Asynchronously receive the next server message.
    pub async fn recv(&mut self) -> Option<ServerMessage> {
        self.outbound_rx.recv().await
    }

    /// Non-blocking receive for immediate assertions.
    pub fn try_recv(&mut self) -> Result<ServerMessage, mpsc::error::TryRecvError> {
        self.outbound_rx.try_recv()
    }
}

impl Drop for MockWsSession {
    fn drop(&mut self) {
        self.broadcaster.unregister_client(&self.client_id);
    }
}
