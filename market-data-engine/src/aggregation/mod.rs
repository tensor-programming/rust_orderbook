//! Market data aggregation module: Candlesticks (OHLCV), Technical Indicators, and OrderBook Analytics.

pub mod candlestick;
pub mod indicators;
pub mod orderbook_snapshot;
pub mod types;

pub use candlestick::{CandleEvent, CandlestickAggregator};
pub use indicators::{
    calculate_atr, calculate_bollinger_bands, calculate_ema, calculate_macd, calculate_rsi,
    calculate_sma, calculate_vwap, calculate_wma, TechnicalIndicatorsEngine,
};
pub use orderbook_snapshot::OrderBookAnalytics;
pub use types::{
    BollingerBandsResult, Candlestick, EnrichedOrderBookSnapshot, MacdResult, StochasticResult,
    TechnicalSnapshot, Timeframe,
};
