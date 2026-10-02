//! JSON WebSocket message definitions for market data streaming.
//!
//! Formatted for 1:1 compatibility with `server.js` and frontend clients.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::aggregation::types::{Candlestick, EnrichedOrderBookSnapshot, TechnicalSnapshot, Timeframe};

/// A single price level in the orderbook.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

impl BookLevel {
    pub fn new(price: Decimal, size: Decimal) -> Self {
        Self { price, size }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Client Incoming Messages
// ═══════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Subscribe to a market/symbol or channel.
    #[serde(rename = "subscribe")]
    Subscribe {
        #[serde(rename = "marketId", alias = "market_id")]
        market_id: Option<String>,
        symbol: Option<String>,
        channel: Option<String>,
        timeframe: Option<String>,
        backpressure: Option<String>,
        #[serde(rename = "throttleMs", alias = "throttle_ms")]
        throttle_ms: Option<u64>,
    },
    /// Unsubscribe from a market/symbol or channel.
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        #[serde(rename = "marketId", alias = "market_id")]
        market_id: Option<String>,
        symbol: Option<String>,
        channel: Option<String>,
        timeframe: Option<String>,
    },
    /// Heartbeat ping.
    #[serde(rename = "ping")]
    Ping,
}

// ═══════════════════════════════════════════════════════════════════
// Server Outgoing Messages
// ═══════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Initial welcome message upon connection.
    #[serde(rename = "connected")]
    Connected { message: String },

    /// Subscription confirmation.
    #[serde(rename = "subscribed")]
    Subscribed {
        #[serde(rename = "marketId", skip_serializing_if = "Option::is_none")]
        market_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        symbol: Option<String>,
        channel: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        timeframe: Option<String>,
    },

    /// Unsubscription confirmation.
    #[serde(rename = "unsubscribed")]
    Unsubscribed {
        #[serde(rename = "marketId", skip_serializing_if = "Option::is_none")]
        market_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        symbol: Option<String>,
        channel: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        timeframe: Option<String>,
    },

    /// Real-time price ticker update.
    #[serde(rename = "price_update")]
    PriceUpdate {
        #[serde(rename = "marketId", skip_serializing_if = "Option::is_none")]
        market_id: Option<String>,
        symbol: String,
        price: Decimal,
        timestamp: i64,
    },

    /// Real-time standard orderbook depth update.
    #[serde(rename = "orderbook_update")]
    OrderBookUpdate {
        #[serde(rename = "marketId", skip_serializing_if = "Option::is_none")]
        market_id: Option<String>,
        symbol: String,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        timestamp: i64,
    },

    /// Real-time enriched orderbook snapshot with analytics (spread, micro-price, imbalance).
    #[serde(rename = "orderbook_enriched")]
    OrderBookEnriched {
        snapshot: EnrichedOrderBookSnapshot,
    },

    /// Real-time candlestick (OHLCV) update or rollover.
    #[serde(rename = "candle_update")]
    CandleUpdate {
        #[serde(rename = "marketId", skip_serializing_if = "Option::is_none")]
        market_id: Option<String>,
        symbol: String,
        timeframe: Timeframe,
        candle: Candlestick,
    },

    /// Real-time technical indicators snapshot.
    #[serde(rename = "indicators_update")]
    IndicatorsUpdate {
        #[serde(rename = "marketId", skip_serializing_if = "Option::is_none")]
        market_id: Option<String>,
        symbol: String,
        timeframe: Timeframe,
        indicators: TechnicalSnapshot,
    },

    /// Real-time trade execution broadcast.
    #[serde(rename = "trade")]
    Trade {
        id: String,
        #[serde(rename = "marketId", skip_serializing_if = "Option::is_none")]
        market_id: Option<String>,
        symbol: String,
        price: Decimal,
        size: Decimal,
        side: String,
        timestamp: i64,
    },

    /// Heartbeat pong response.
    #[serde(rename = "pong")]
    Pong { timestamp: i64 },

    /// Error notification.
    #[serde(rename = "error")]
    Error { message: String },
}
