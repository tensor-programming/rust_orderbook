//! Domain types for market data aggregation: Timeframes, Candlesticks, Indicators, and OrderBook Analytics.

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::messages::BookLevel;

// ═══════════════════════════════════════════════════════════════════
// Timeframe
// ═══════════════════════════════════════════════════════════════════

/// Timeframe resolution for candlestick aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Timeframe {
    #[serde(rename = "1s")]
    OneSecond,
    #[serde(rename = "1m")]
    OneMinute,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "15m")]
    FifteenMinutes,
    #[serde(rename = "30m")]
    ThirtyMinutes,
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "4h")]
    FourHours,
    #[serde(rename = "1d")]
    OneDay,
    #[serde(rename = "1w")]
    OneWeek,
}

impl Timeframe {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "1s" | "sec" | "second" => Some(Timeframe::OneSecond),
            "1m" | "min" | "minute" => Some(Timeframe::OneMinute),
            "5m" => Some(Timeframe::FiveMinutes),
            "15m" => Some(Timeframe::FifteenMinutes),
            "30m" => Some(Timeframe::ThirtyMinutes),
            "1h" | "hour" => Some(Timeframe::OneHour),
            "4h" => Some(Timeframe::FourHours),
            "1d" | "day" => Some(Timeframe::OneDay),
            "1w" | "week" => Some(Timeframe::OneWeek),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Timeframe::OneSecond => "1s",
            Timeframe::OneMinute => "1m",
            Timeframe::FiveMinutes => "5m",
            Timeframe::FifteenMinutes => "15m",
            Timeframe::ThirtyMinutes => "30m",
            Timeframe::OneHour => "1h",
            Timeframe::FourHours => "4h",
            Timeframe::OneDay => "1d",
            Timeframe::OneWeek => "1w",
        }
    }

    pub fn duration_millis(&self) -> i64 {
        match self {
            Timeframe::OneSecond => 1_000,
            Timeframe::OneMinute => 60_000,
            Timeframe::FiveMinutes => 300_000,
            Timeframe::FifteenMinutes => 900_000,
            Timeframe::ThirtyMinutes => 1_800_000,
            Timeframe::OneHour => 3_600_000,
            Timeframe::FourHours => 14_400_000,
            Timeframe::OneDay => 86_400_000,
            Timeframe::OneWeek => 604_800_000,
        }
    }

    /// Align an arbitrary UTC timestamp to the start boundary of this timeframe.
    pub fn align_timestamp(&self, timestamp: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Timeframe::OneSecond => timestamp.with_nanosecond(0).unwrap_or(timestamp),
            Timeframe::OneMinute => timestamp
                .with_second(0)
                .unwrap_or(timestamp)
                .with_nanosecond(0)
                .unwrap_or(timestamp),
            Timeframe::FiveMinutes => {
                let aligned_min = (timestamp.minute() / 5) * 5;
                timestamp
                    .with_minute(aligned_min)
                    .unwrap_or(timestamp)
                    .with_second(0)
                    .unwrap_or(timestamp)
                    .with_nanosecond(0)
                    .unwrap_or(timestamp)
            }
            Timeframe::FifteenMinutes => {
                let aligned_min = (timestamp.minute() / 15) * 15;
                timestamp
                    .with_minute(aligned_min)
                    .unwrap_or(timestamp)
                    .with_second(0)
                    .unwrap_or(timestamp)
                    .with_nanosecond(0)
                    .unwrap_or(timestamp)
            }
            Timeframe::ThirtyMinutes => {
                let aligned_min = (timestamp.minute() / 30) * 30;
                timestamp
                    .with_minute(aligned_min)
                    .unwrap_or(timestamp)
                    .with_second(0)
                    .unwrap_or(timestamp)
                    .with_nanosecond(0)
                    .unwrap_or(timestamp)
            }
            Timeframe::OneHour => timestamp
                .with_minute(0)
                .unwrap_or(timestamp)
                .with_second(0)
                .unwrap_or(timestamp)
                .with_nanosecond(0)
                .unwrap_or(timestamp),
            Timeframe::FourHours => {
                let aligned_hr = (timestamp.hour() / 4) * 4;
                timestamp
                    .with_hour(aligned_hr)
                    .unwrap_or(timestamp)
                    .with_minute(0)
                    .unwrap_or(timestamp)
                    .with_second(0)
                    .unwrap_or(timestamp)
                    .with_nanosecond(0)
                    .unwrap_or(timestamp)
            }
            Timeframe::OneDay => timestamp
                .with_hour(0)
                .unwrap_or(timestamp)
                .with_minute(0)
                .unwrap_or(timestamp)
                .with_second(0)
                .unwrap_or(timestamp)
                .with_nanosecond(0)
                .unwrap_or(timestamp),
            Timeframe::OneWeek => {
                let days_from_monday = timestamp.weekday().num_days_from_monday();
                let week_start = timestamp - Duration::days(days_from_monday as i64);
                week_start
                    .with_hour(0)
                    .unwrap_or(week_start)
                    .with_minute(0)
                    .unwrap_or(week_start)
                    .with_second(0)
                    .unwrap_or(week_start)
                    .with_nanosecond(0)
                    .unwrap_or(week_start)
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Candlestick (OHLCV)
// ═══════════════════════════════════════════════════════════════════

/// Aggregated OHLCV Candlestick data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Candlestick {
    pub symbol: String,
    pub timeframe: Timeframe,
    #[serde(rename = "timestamp")]
    pub timestamp_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: Decimal,
    #[serde(rename = "tradeCount")]
    pub trade_count: u64,
    #[serde(rename = "isClosed")]
    pub is_closed: bool,
}

impl Candlestick {
    pub fn new(
        symbol: impl Into<String>,
        timeframe: Timeframe,
        timestamp_ms: i64,
        price: Decimal,
        volume: Decimal,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            timeframe,
            timestamp_ms,
            open: price,
            high: price,
            low: price,
            close: price,
            volume,
            quote_volume: price * volume,
            trade_count: 1,
            is_closed: false,
        }
    }

    /// Ingest a trade execution into this candlestick.
    pub fn update(&mut self, price: Decimal, quantity: Decimal) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.volume += quantity;
        self.quote_volume += price * quantity;
        self.trade_count += 1;
    }
}

// ═══════════════════════════════════════════════════════════════════
// Technical Indicators Models
// ═══════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MacdResult {
    #[serde(rename = "macdLine")]
    pub macd_line: Decimal,
    #[serde(rename = "signalLine")]
    pub signal_line: Decimal,
    pub histogram: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BollingerBandsResult {
    pub upper: Decimal,
    pub middle: Decimal,
    pub lower: Decimal,
    pub bandwidth: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StochasticResult {
    #[serde(rename = "k")]
    pub k: Decimal,
    #[serde(rename = "d")]
    pub d: Decimal,
}

/// Comprehensive Technical Indicators Snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TechnicalSnapshot {
    pub symbol: String,
    pub timeframe: Timeframe,
    #[serde(rename = "timestamp")]
    pub timestamp_ms: i64,
    #[serde(rename = "currentPrice")]
    pub current_price: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sma_20: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sma_50: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ema_12: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ema_26: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rsi_14: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub macd: Option<MacdResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bollinger_bands: Option<BollingerBandsResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vwap: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atr_14: Option<Decimal>,
}

// ═══════════════════════════════════════════════════════════════════
// Enriched Orderbook Analytics
// ═══════════════════════════════════════════════════════════════════

/// Enriched real-time OrderBook snapshot with analytics metrics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnrichedOrderBookSnapshot {
    pub symbol: String,
    #[serde(rename = "marketId", skip_serializing_if = "Option::is_none")]
    pub market_id: Option<String>,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    #[serde(rename = "bestBid")]
    pub best_bid: Option<Decimal>,
    #[serde(rename = "bestAsk")]
    pub best_ask: Option<Decimal>,
    pub spread: Option<Decimal>,
    #[serde(rename = "spreadPercentage")]
    pub spread_percentage: Option<Decimal>,
    #[serde(rename = "midPrice")]
    pub mid_price: Option<Decimal>,
    #[serde(rename = "microPrice")]
    pub micro_price: Option<Decimal>,
    #[serde(rename = "bidDepth")]
    pub bid_depth: Decimal,
    #[serde(rename = "askDepth")]
    pub ask_depth: Decimal,
    #[serde(rename = "orderImbalance")]
    pub order_imbalance: Option<Decimal>,
    #[serde(rename = "timestamp")]
    pub timestamp_ms: i64,
}
