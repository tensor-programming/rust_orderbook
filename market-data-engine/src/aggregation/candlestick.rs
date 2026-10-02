//! Real-time multi-timeframe candlestick aggregator.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;

use super::types::{Candlestick, Timeframe};

/// Event emitted by the candlestick aggregator when a candle updates or closes.
#[derive(Debug, Clone, PartialEq)]
pub enum CandleEvent {
    /// In-progress candle updated with a new trade.
    Updated(Candlestick),
    /// A candle finalized and rolled over into a new one.
    Closed {
        closed_candle: Candlestick,
        new_candle: Candlestick,
    },
}

/// Internal state for a specific (symbol, timeframe) pair.
struct SeriesState {
    active_candle: Option<Candlestick>,
    history: VecDeque<Candlestick>,
    max_history: usize,
}

impl SeriesState {
    fn new(max_history: usize) -> Self {
        Self {
            active_candle: None,
            history: VecDeque::with_capacity(max_history),
            max_history,
        }
    }
}

/// Real-time multi-symbol, multi-timeframe candlestick aggregator.
pub struct CandlestickAggregator {
    timeframes: Vec<Timeframe>,
    /// (symbol, timeframe) -> SeriesState
    series: HashMap<(String, Timeframe), SeriesState>,
    max_history: usize,
}

impl CandlestickAggregator {
    pub fn new(timeframes: Vec<Timeframe>) -> Self {
        Self {
            timeframes,
            series: HashMap::new(),
            max_history: 500,
        }
    }

    /// Create an aggregator configured with standard timeframes: 1s, 1m, 5m, 15m, 1h, 1d.
    pub fn with_standard_timeframes() -> Self {
        Self::new(vec![
            Timeframe::OneSecond,
            Timeframe::OneMinute,
            Timeframe::FiveMinutes,
            Timeframe::FifteenMinutes,
            Timeframe::OneHour,
            Timeframe::OneDay,
        ])
    }

    /// Set maximum number of historical candles to keep in memory per series.
    pub fn set_max_history(&mut self, max: usize) {
        self.max_history = max;
    }

    /// Ingest a trade execution and update/rollover candlesticks across all active timeframes.
    pub fn ingest_trade(
        &mut self,
        symbol: &str,
        price: Decimal,
        quantity: Decimal,
        timestamp: DateTime<Utc>,
    ) -> Vec<CandleEvent> {
        let mut events = Vec::new();

        for tf in &self.timeframes {
            let key = (symbol.to_string(), *tf);
            let state = self
                .series
                .entry(key)
                .or_insert_with(|| SeriesState::new(self.max_history));

            let aligned_boundary = tf.align_timestamp(timestamp);
            let boundary_ms = aligned_boundary.timestamp_millis();

            match &mut state.active_candle {
                Some(active) if active.timestamp_ms == boundary_ms => {
                    // Update current active candle
                    active.update(price, quantity);
                    events.push(CandleEvent::Updated(active.clone()));
                }
                Some(active) => {
                    // Boundary crossed -> close previous candle and start new one
                    let mut closed = active.clone();
                    closed.is_closed = true;

                    // Push to history buffer
                    if state.history.len() >= state.max_history {
                        state.history.pop_front();
                    }
                    state.history.push_back(closed.clone());

                    // Start new active candle
                    let new_candle =
                        Candlestick::new(symbol, *tf, boundary_ms, price, quantity);
                    *active = new_candle.clone();

                    events.push(CandleEvent::Closed {
                        closed_candle: closed,
                        new_candle,
                    });
                }
                None => {
                    // First candle in series
                    let new_candle =
                        Candlestick::new(symbol, *tf, boundary_ms, price, quantity);
                    state.active_candle = Some(new_candle.clone());
                    events.push(CandleEvent::Updated(new_candle));
                }
            }
        }

        events
    }

    /// Ingest trade with timestamp in unix milliseconds.
    pub fn ingest_trade_ms(
        &mut self,
        symbol: &str,
        price: Decimal,
        quantity: Decimal,
        timestamp_ms: i64,
    ) -> Vec<CandleEvent> {
        let timestamp = Utc
            .timestamp_millis_opt(timestamp_ms)
            .single()
            .unwrap_or_else(Utc::now);
        self.ingest_trade(symbol, price, quantity, timestamp)
    }

    /// Retrieve the current active (in-progress) candle for a symbol and timeframe.
    pub fn get_active_candle(&self, symbol: &str, timeframe: Timeframe) -> Option<&Candlestick> {
        let key = (symbol.to_string(), timeframe);
        self.series.get(&key).and_then(|s| s.active_candle.as_ref())
    }

    /// Retrieve historical closed candles for a symbol and timeframe.
    pub fn get_history(&self, symbol: &str, timeframe: Timeframe) -> Vec<Candlestick> {
        let key = (symbol.to_string(), timeframe);
        self.series
            .get(&key)
            .map(|s| s.history.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Retrieve historical closed candles combined with current active candle.
    pub fn get_all_candles(&self, symbol: &str, timeframe: Timeframe) -> Vec<Candlestick> {
        let key = (symbol.to_string(), timeframe);
        if let Some(state) = self.series.get(&key) {
            let mut list: Vec<Candlestick> = state.history.iter().cloned().collect();
            if let Some(active) = &state.active_candle {
                list.push(active.clone());
            }
            list
        } else {
            Vec::new()
        }
    }
}
