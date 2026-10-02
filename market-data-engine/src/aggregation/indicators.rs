//! Financial algorithms for technical indicators using Decimal arithmetic.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::types::{BollingerBandsResult, Candlestick, MacdResult, TechnicalSnapshot, Timeframe};

// ═══════════════════════════════════════════════════════════════════
// Pure Technical Indicator Functions
// ═══════════════════════════════════════════════════════════════════

/// Simple Moving Average (SMA).
pub fn calculate_sma(prices: &[Decimal], period: usize) -> Option<Decimal> {
    if prices.len() < period || period == 0 {
        return None;
    }
    let slice = &prices[prices.len() - period..];
    let sum: Decimal = slice.iter().sum();
    Some(sum / Decimal::from(period))
}

/// Exponential Moving Average (EMA).
pub fn calculate_ema(prices: &[Decimal], period: usize) -> Option<Decimal> {
    if prices.len() < period || period == 0 {
        return None;
    }

    // Initial SMA as baseline
    let initial_slice = &prices[..period];
    let mut ema: Decimal = initial_slice.iter().sum::<Decimal>() / Decimal::from(period);

    // Multiplier: 2 / (period + 1)
    let multiplier = Decimal::TWO / Decimal::from(period + 1);

    for price in &prices[period..] {
        ema = (*price - ema) * multiplier + ema;
    }

    Some(ema)
}

/// Weighted Moving Average (WMA).
pub fn calculate_wma(prices: &[Decimal], period: usize) -> Option<Decimal> {
    if prices.len() < period || period == 0 {
        return None;
    }

    let slice = &prices[prices.len() - period..];
    let mut weighted_sum = Decimal::ZERO;
    let weight_sum = Decimal::from(period * (period + 1) / 2);

    for (i, price) in slice.iter().enumerate() {
        let weight = Decimal::from(i + 1);
        weighted_sum += *price * weight;
    }

    Some(weighted_sum / weight_sum)
}

/// Relative Strength Index (RSI - Wilder's Smoothing / Standard).
pub fn calculate_rsi(prices: &[Decimal], period: usize) -> Option<Decimal> {
    if prices.len() <= period || period == 0 {
        return None;
    }

    let mut gains = Vec::with_capacity(prices.len() - 1);
    let mut losses = Vec::with_capacity(prices.len() - 1);

    for i in 1..prices.len() {
        let diff = prices[i] - prices[i - 1];
        if diff > Decimal::ZERO {
            gains.push(diff);
            losses.push(Decimal::ZERO);
        } else {
            gains.push(Decimal::ZERO);
            losses.push(-diff);
        }
    }

    if gains.len() < period {
        return None;
    }

    // Initial averages
    let mut avg_gain: Decimal = gains[..period].iter().sum::<Decimal>() / Decimal::from(period);
    let mut avg_loss: Decimal = losses[..period].iter().sum::<Decimal>() / Decimal::from(period);

    // Smoothed averages
    let p_minus_one = Decimal::from(period - 1);
    let p = Decimal::from(period);

    for i in period..gains.len() {
        avg_gain = (avg_gain * p_minus_one + gains[i]) / p;
        avg_loss = (avg_loss * p_minus_one + losses[i]) / p;
    }

    if avg_loss == Decimal::ZERO {
        return Some(dec!(100));
    }

    let rs = avg_gain / avg_loss;
    let rsi = dec!(100) - (dec!(100) / (Decimal::ONE + rs));
    Some(rsi)
}

/// Moving Average Convergence Divergence (MACD).
pub fn calculate_macd(
    prices: &[Decimal],
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> Option<MacdResult> {
    if prices.len() < slow_period + signal_period || fast_period >= slow_period {
        return None;
    }

    // Compute series of MACD values (fast_ema - slow_ema)
    let mut macd_series = Vec::new();
    let multiplier_fast = Decimal::TWO / Decimal::from(fast_period + 1);
    let multiplier_slow = Decimal::TWO / Decimal::from(slow_period + 1);

    // Initial SMAs
    let mut fast_ema = prices[..fast_period].iter().sum::<Decimal>() / Decimal::from(fast_period);
    let mut slow_ema = prices[..slow_period].iter().sum::<Decimal>() / Decimal::from(slow_period);

    // Advance fast EMA to align with slow EMA starting point
    for p in &prices[fast_period..slow_period] {
        fast_ema = (*p - fast_ema) * multiplier_fast + fast_ema;
    }

    // Step together and collect MACD line values
    for p in &prices[slow_period..] {
        fast_ema = (*p - fast_ema) * multiplier_fast + fast_ema;
        slow_ema = (*p - slow_ema) * multiplier_slow + slow_ema;
        macd_series.push(fast_ema - slow_ema);
    }

    if macd_series.len() < signal_period {
        return None;
    }

    let macd_line = *macd_series.last()?;
    let signal_line = calculate_ema(&macd_series, signal_period)?;
    let histogram = macd_line - signal_line;

    Some(MacdResult {
        macd_line,
        signal_line,
        histogram,
    })
}

/// Bollinger Bands.
pub fn calculate_bollinger_bands(
    prices: &[Decimal],
    period: usize,
    std_multiplier: Decimal,
) -> Option<BollingerBandsResult> {
    if prices.len() < period || period == 0 {
        return None;
    }

    let sma = calculate_sma(prices, period)?;
    let slice = &prices[prices.len() - period..];

    // Variance calculation
    let mut sum_sq_diff = Decimal::ZERO;
    for price in slice {
        let diff = *price - sma;
        sum_sq_diff += diff * diff;
    }
    let variance = sum_sq_diff / Decimal::from(period);

    // Standard deviation using f64 approximation for sqrt then back to Decimal
    let var_f64 = variance.to_f64()?;
    let std_dev_f64 = var_f64.sqrt();
    let std_dev = Decimal::from_f64_retain(std_dev_f64)?;

    let band_offset = std_dev * std_multiplier;
    let upper = sma + band_offset;
    let lower = sma - band_offset;
    let bandwidth = if sma != Decimal::ZERO {
        (upper - lower) / sma
    } else {
        Decimal::ZERO
    };

    Some(BollingerBandsResult {
        upper,
        middle: sma,
        lower,
        bandwidth,
    })
}

/// Volume Weighted Average Price (VWAP).
pub fn calculate_vwap(candles: &[Candlestick]) -> Option<Decimal> {
    if candles.is_empty() {
        return None;
    }

    let mut total_quote = Decimal::ZERO;
    let mut total_volume = Decimal::ZERO;

    for candle in candles {
        // Typical price = (High + Low + Close) / 3
        let typical_price = (candle.high + candle.low + candle.close) / Decimal::from(3);
        total_quote += typical_price * candle.volume;
        total_volume += candle.volume;
    }

    if total_volume == Decimal::ZERO {
        return None;
    }

    Some(total_quote / total_volume)
}

/// Average True Range (ATR).
pub fn calculate_atr(candles: &[Candlestick], period: usize) -> Option<Decimal> {
    if candles.len() <= period || period == 0 {
        return None;
    }

    let mut tr_list = Vec::with_capacity(candles.len() - 1);
    for i in 1..candles.len() {
        let current = &candles[i];
        let prev_close = candles[i - 1].close;

        let hl = current.high - current.low;
        let h_pc = (current.high - prev_close).abs();
        let l_pc = (current.low - prev_close).abs();

        let tr = hl.max(h_pc).max(l_pc);
        tr_list.push(tr);
    }

    if tr_list.len() < period {
        return None;
    }

    // Initial ATR
    let mut atr: Decimal = tr_list[..period].iter().sum::<Decimal>() / Decimal::from(period);
    let p_minus_one = Decimal::from(period - 1);
    let p = Decimal::from(period);

    // Smoothed ATR (Wilder's)
    for tr in &tr_list[period..] {
        atr = (atr * p_minus_one + *tr) / p;
    }

    Some(atr)
}

// ═══════════════════════════════════════════════════════════════════
// Technical Indicators Engine
// ═══════════════════════════════════════════════════════════════════

/// High-level technical indicators computation engine.
pub struct TechnicalIndicatorsEngine;

impl TechnicalIndicatorsEngine {
    /// Compute a complete technical indicators snapshot for a series of candlesticks.
    pub fn compute_snapshot(
        symbol: &str,
        timeframe: Timeframe,
        candles: &[Candlestick],
    ) -> Option<TechnicalSnapshot> {
        let last_candle = candles.last()?;
        let close_prices: Vec<Decimal> = candles.iter().map(|c| c.close).collect();

        let sma_20 = calculate_sma(&close_prices, 20);
        let sma_50 = calculate_sma(&close_prices, 50);
        let ema_12 = calculate_ema(&close_prices, 12);
        let ema_26 = calculate_ema(&close_prices, 26);
        let rsi_14 = calculate_rsi(&close_prices, 14);
        let macd = calculate_macd(&close_prices, 12, 26, 9);
        let bollinger_bands = calculate_bollinger_bands(&close_prices, 20, Decimal::TWO);
        let vwap = calculate_vwap(candles);
        let atr_14 = calculate_atr(candles, 14);

        Some(TechnicalSnapshot {
            symbol: symbol.to_string(),
            timeframe,
            timestamp_ms: last_candle.timestamp_ms,
            current_price: last_candle.close,
            sma_20,
            sma_50,
            ema_12,
            ema_26,
            rsi_14,
            macd,
            bollinger_bands,
            vwap,
            atr_14,
        })
    }
}
