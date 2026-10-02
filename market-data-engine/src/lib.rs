//! # Market Data Service
//!
//! Real-time market data distribution service providing WebSocket streaming,
//! PubSub event bus with backpressure handling, multi-timeframe candlestick (OHLCV)
//! aggregation, and technical indicators.
//!
//! Compatible with the client/server protocols used across the project, `trading-common`, and `server.js`.

pub mod aggregation;
pub mod broadcaster;
pub mod messages;
pub mod pubsub;
pub mod server;
pub mod subscription;

pub use aggregation::{
    calculate_atr, calculate_bollinger_bands, calculate_ema, calculate_macd, calculate_rsi,
    calculate_sma, calculate_vwap, calculate_wma, BollingerBandsResult, CandleEvent, Candlestick,
    CandlestickAggregator, EnrichedOrderBookSnapshot, MacdResult, OrderBookAnalytics,
    StochasticResult, TechnicalIndicatorsEngine, TechnicalSnapshot, Timeframe,
};
pub use broadcaster::MarketDataBroadcaster;
pub use messages::{BookLevel, ClientMessage, ServerMessage};
pub use pubsub::{BackpressureMetrics, BackpressureQueue, BackpressureStrategy, MarketDataPubSub};
pub use server::{MarketDataWsServer, MockWsSession, WsServerConfig};
pub use subscription::{Channel, SubscriptionManager, Topic};
