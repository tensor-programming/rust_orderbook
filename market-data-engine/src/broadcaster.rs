//! High-throughput market data broadcaster distributing updates to subscribed WebSocket clients
//! with backpressure management.

use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::aggregation::types::{Candlestick, EnrichedOrderBookSnapshot, TechnicalSnapshot, Timeframe};
use crate::messages::{BookLevel, ServerMessage};
use crate::pubsub::backpressure::{BackpressureQueue, BackpressureStrategy};
use crate::pubsub::engine::MarketDataPubSub;
use crate::subscription::{Channel, SubscriptionManager};

/// Broadcast hub managing client outbound channels, topic dispatching, and backpressure.
pub struct MarketDataBroadcaster {
    subscriptions: Arc<SubscriptionManager>,
    pubsub: Arc<MarketDataPubSub>,
    /// client_id -> outbound message channel sender (raw mpsc)
    senders: DashMap<Uuid, mpsc::Sender<ServerMessage>>,
    /// client_id -> BackpressureQueue (for advanced backpressured consumers)
    backpressure_queues: DashMap<Uuid, Arc<BackpressureQueue<ServerMessage>>>,
}

impl MarketDataBroadcaster {
    pub fn new(subscriptions: Arc<SubscriptionManager>) -> Self {
        let pubsub = Arc::new(MarketDataPubSub::new(Arc::clone(&subscriptions)));
        Self {
            subscriptions,
            pubsub,
            senders: DashMap::new(),
            backpressure_queues: DashMap::new(),
        }
    }

    /// Access the underlying PubSub broker.
    pub fn pubsub(&self) -> Arc<MarketDataPubSub> {
        Arc::clone(&self.pubsub)
    }

    /// Register an active client channel with default backpressure.
    pub fn register_client(&self, client_id: Uuid, sender: mpsc::Sender<ServerMessage>) {
        self.senders.insert(client_id, sender);
        self.subscriptions.register_client(client_id);
    }

    /// Register a client with a dedicated backpressure queue.
    pub fn register_backpressured_client(
        &self,
        client_id: Uuid,
        capacity: usize,
        strategy: BackpressureStrategy,
    ) -> Arc<BackpressureQueue<ServerMessage>> {
        let queue = self.pubsub.register_subscriber(client_id, capacity, strategy);
        self.backpressure_queues.insert(client_id, Arc::clone(&queue));
        queue
    }

    /// Unregister a disconnected client.
    pub fn unregister_client(&self, client_id: &Uuid) {
        self.senders.remove(client_id);
        self.backpressure_queues.remove(client_id);
        self.pubsub.unregister_subscriber(client_id);
        self.subscriptions.unregister_client(client_id);
    }

    /// Send a message directly to a single client.
    pub async fn send_to_client(&self, client_id: &Uuid, msg: ServerMessage) {
        if let Some(sender) = self.senders.get(client_id) {
            if let Err(e) = sender.send(msg).await {
                debug!(%client_id, "Failed to send message to client (disconnected): {}", e);
            }
        }
    }

    /// Broadcast price update to all clients subscribed to prices or all.
    pub async fn broadcast_price_update(
        &self,
        symbol: &str,
        market_id: Option<String>,
        price: Decimal,
    ) {
        let timestamp = Utc::now().timestamp_millis();
        let target = market_id.as_deref().unwrap_or(symbol);
        let subscribers = self.subscriptions.get_subscribers_for(target, Channel::Prices);

        if subscribers.is_empty() {
            return;
        }

        let msg = ServerMessage::PriceUpdate {
            market_id,
            symbol: symbol.to_string(),
            price,
            timestamp,
        };

        self.dispatch_to_subscribers(&subscribers, msg).await;
    }

    /// Broadcast orderbook depth update to all clients subscribed to orderbook.
    pub async fn broadcast_orderbook_update(
        &self,
        symbol: &str,
        market_id: Option<String>,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
    ) {
        let timestamp = Utc::now().timestamp_millis();
        let target = market_id.as_deref().unwrap_or(symbol);
        let subscribers = self
            .subscriptions
            .get_subscribers_for(target, Channel::OrderBook);

        if subscribers.is_empty() {
            return;
        }

        let msg = ServerMessage::OrderBookUpdate {
            market_id,
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp,
        };

        self.dispatch_to_subscribers(&subscribers, msg).await;
    }

    /// Broadcast enriched orderbook snapshot with analytics (mid-price, micro-price, imbalance).
    pub async fn broadcast_enriched_orderbook(&self, snapshot: EnrichedOrderBookSnapshot) {
        let target = snapshot
            .market_id
            .as_deref()
            .unwrap_or(&snapshot.symbol);
        let subscribers = self
            .subscriptions
            .get_subscribers_for(target, Channel::EnrichedOrderBook);

        if subscribers.is_empty() {
            return;
        }

        let msg = ServerMessage::OrderBookEnriched { snapshot };
        self.dispatch_to_subscribers(&subscribers, msg).await;
    }

    /// Broadcast candlestick update/rollover to all clients subscribed to candles.
    pub async fn broadcast_candle_update(
        &self,
        symbol: &str,
        market_id: Option<String>,
        timeframe: Timeframe,
        candle: Candlestick,
    ) {
        let target = market_id.as_deref().unwrap_or(symbol);
        let subscribers = self
            .subscriptions
            .get_subscribers_for(target, Channel::Candles(Some(timeframe)));

        if subscribers.is_empty() {
            return;
        }

        let msg = ServerMessage::CandleUpdate {
            market_id,
            symbol: symbol.to_string(),
            timeframe,
            candle,
        };

        self.dispatch_to_subscribers(&subscribers, msg).await;
    }

    /// Broadcast technical indicators snapshot update to all clients subscribed to indicators.
    pub async fn broadcast_indicators_update(
        &self,
        symbol: &str,
        market_id: Option<String>,
        timeframe: Timeframe,
        indicators: TechnicalSnapshot,
    ) {
        let target = market_id.as_deref().unwrap_or(symbol);
        let subscribers = self
            .subscriptions
            .get_subscribers_for(target, Channel::Indicators(Some(timeframe)));

        if subscribers.is_empty() {
            return;
        }

        let msg = ServerMessage::IndicatorsUpdate {
            market_id,
            symbol: symbol.to_string(),
            timeframe,
            indicators,
        };

        self.dispatch_to_subscribers(&subscribers, msg).await;
    }

    /// Broadcast trade execution to all clients subscribed to trades.
    pub async fn broadcast_trade(
        &self,
        id: &str,
        symbol: &str,
        market_id: Option<String>,
        price: Decimal,
        size: Decimal,
        side: &str,
    ) {
        let timestamp = Utc::now().timestamp_millis();
        let target = market_id.as_deref().unwrap_or(symbol);
        let subscribers = self.subscriptions.get_subscribers_for(target, Channel::Trades);

        if subscribers.is_empty() {
            return;
        }

        let msg = ServerMessage::Trade {
            id: id.to_string(),
            market_id,
            symbol: symbol.to_string(),
            price,
            size,
            side: side.to_string(),
            timestamp,
        };

        self.dispatch_to_subscribers(&subscribers, msg).await;
    }

    /// Internal dispatch helper to send message to list of clients concurrently,
    /// enforcing backpressure policies.
    async fn dispatch_to_subscribers(&self, subscribers: &[Uuid], msg: ServerMessage) {
        for client_id in subscribers {
            // Push to backpressured queue if configured
            if let Some(queue) = self.backpressure_queues.get(client_id) {
                if !queue.push(msg.clone()) {
                    debug!(%client_id, "Frame dropped on client backpressure queue (buffer full)");
                }
            }

            // Also dispatch to raw mpsc channel
            if let Some(sender) = self.senders.get(client_id) {
                if let Err(mpsc::error::TrySendError::Full(_)) = sender.try_send(msg.clone()) {
                    warn!(%client_id, "Client outbound queue full, dropping frame to preserve throughput");
                }
            }
        }
    }
}
