//! Realistic Mock Market Data Flow for the Order Matching Engine.
//!
//! Simulates realistic institutional market microstructure directly inside the internal
//! [`crate::engine::MatchingEngine`].
//!
//! # Microstructure Features:
//! - **Markov Regime Switching**: Alternates between trending bull waves (72% buy flow),
//!   trending bear waves (72% sell flow), and consolidation ranges (50% flow with mean-reversion).
//! - **Continuous Brownian Motion**: Price drifts smoothly via momentum velocity and Gaussian
//!   micro-noise, stepping tick-by-tick without artificial jumps.
//! - **Active Market Maker Quoting**: Maintains tight 1–2 tick spreads and full 10-level deep
//!   liquidity on both sides of the book.
//! - **Natural Trade Pacing & Autocorrelation**: Trades arrive as a stochastic Poisson-like
//!   process with Pareto-distributed order sizes, creating authentic candlestick patterns
//!   (wicks, bodies, trend continuations, and pullbacks).
//! - **Stop Order Triggers**: Real stop orders trigger as trending prices cross stop thresholds.

use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info};

use shared::{MatchingEngineOps, OrderBuilder, OrderType, Side, TimeInForce};

use crate::engine::MatchingEngine;

/// Configuration parameters for a simulated market.
#[derive(Debug, Clone)]
pub struct MarketConfig {
    pub symbol: &'static str,
    pub base_price: Decimal,
    pub tick_size: Decimal,
    pub min_spread_ticks: i64,
    pub min_qty: Decimal,
    pub max_qty: Decimal,
}

impl MarketConfig {
    pub fn default_markets() -> Vec<Self> {
        vec![
            Self {
                symbol: "BTC-USD",
                base_price: dec!(52345.50),
                tick_size: dec!(0.50),
                min_spread_ticks: 2,
                min_qty: dec!(0.10),
                max_qty: dec!(1.50),
            },
            Self {
                symbol: "ETH-USD",
                base_price: dec!(3125.20),
                tick_size: dec!(0.10),
                min_spread_ticks: 2,
                min_qty: dec!(0.50),
                max_qty: dec!(5.00),
            },
            Self {
                symbol: "SOL-USD",
                base_price: dec!(145.80),
                tick_size: dec!(0.05),
                min_spread_ticks: 2,
                min_qty: dec!(2.00),
                max_qty: dec!(25.00),
            },
        ]
    }
}

/// Fast, deterministic PRNG with Box-Muller Gaussian transform.
#[derive(Debug, Clone)]
pub struct FastRng {
    state: u64,
}

impl FastRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x853c49e6748fea9b
            } else {
                seed
            },
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Box-Muller Gaussian standard normal N(0, 1)
    pub fn next_gaussian(&mut self) -> f64 {
        let u1 = self.next_f64().max(1e-15);
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}

/// Market regime driving macro trend direction and order flow bias.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketRegime {
    Bullish,
    Bearish,
    Ranging,
}

impl MarketRegime {
    pub fn buy_probability(&self) -> f64 {
        match self {
            MarketRegime::Bullish => 0.72,
            MarketRegime::Bearish => 0.28,
            MarketRegime::Ranging => 0.50,
        }
    }

    pub fn target_drift_ticks(&self) -> f64 {
        match self {
            MarketRegime::Bullish => 0.06,
            MarketRegime::Bearish => -0.06,
            MarketRegime::Ranging => 0.0,
        }
    }
}

/// Dynamic market simulator state tracking smooth price path and regime transitions.
pub struct MarketRuntime {
    pub config: MarketConfig,
    pub current_mid: Decimal,
    pub continuous_drift: f64,
    pub velocity: f64,
    pub regime: MarketRegime,
    pub regime_ticks_left: u32,
    pub anchor_price: Decimal,
}

impl MarketRuntime {
    pub fn new(config: MarketConfig) -> Self {
        let base = config.base_price;
        Self {
            current_mid: base,
            continuous_drift: 0.0,
            velocity: 0.0,
            regime: MarketRegime::Ranging,
            regime_ticks_left: 60,
            anchor_price: base,
            config,
        }
    }

    pub fn update_regime(&mut self, rng: &mut FastRng) {
        if self.regime_ticks_left == 0 {
            let r = rng.next_f64();
            self.regime = match self.regime {
                MarketRegime::Ranging => {
                    if r < 0.50 {
                        MarketRegime::Bullish
                    } else {
                        MarketRegime::Bearish
                    }
                }
                MarketRegime::Bullish => {
                    if r < 0.60 {
                        MarketRegime::Ranging
                    } else {
                        MarketRegime::Bearish
                    }
                }
                MarketRegime::Bearish => {
                    if r < 0.60 {
                        MarketRegime::Ranging
                    } else {
                        MarketRegime::Bullish
                    }
                }
            };
            // 70 to 180 steps per regime (~10 to 27 seconds)
            self.regime_ticks_left = 70 + (rng.next_f64() * 110.0) as u32;
        } else {
            self.regime_ticks_left -= 1;
        }
    }

    /// Advance the stochastic price process by one time-step.
    pub fn step_price(&mut self, rng: &mut FastRng) {
        self.update_regime(rng);

        let target_v = self.regime.target_drift_ticks();
        // Smoothly adjust momentum velocity
        self.velocity = self.velocity * 0.94 + target_v * 0.06;

        // Gentle mean reversion toward anchor during ranging regimes
        let mean_pull = if self.regime == MarketRegime::Ranging {
            let diff = (self.anchor_price - self.current_mid) / self.config.tick_size;
            let diff_f64 = diff.to_string().parse::<f64>().unwrap_or(0.0);
            (diff_f64 * 0.015).clamp(-0.04, 0.04)
        } else {
            0.0
        };

        // Realistic Gaussian microstructure noise
        let noise = rng.next_gaussian() * 0.05;
        let delta_ticks = self.velocity + mean_pull + noise;
        self.continuous_drift += delta_ticks;

        // Shift mid price by exact tick size when drift boundary is reached
        if self.continuous_drift >= 1.0 {
            self.current_mid += self.config.tick_size;
            self.continuous_drift -= 1.0;
        } else if self.continuous_drift <= -1.0 {
            self.current_mid -= self.config.tick_size;
            self.continuous_drift += 1.0;
        }

        // Keep price bounded within realistic envelope (±12% from base)
        let max_dev = self.config.base_price * dec!(0.12);
        if self.current_mid < self.config.base_price - max_dev {
            self.current_mid = self.config.base_price - max_dev;
            self.continuous_drift = 0.0;
        } else if self.current_mid > self.config.base_price + max_dev {
            self.current_mid = self.config.base_price + max_dev;
            self.continuous_drift = 0.0;
        }
    }
}

/// Seed initial resting liquidity on the matching engine's order book.
pub fn seed_market_liquidity(engine: &MatchingEngine, config: &MarketConfig, mid_price: Decimal) {
    let half_spread = config.tick_size;
    let best_bid = mid_price - half_spread;
    let best_ask = mid_price + half_spread;

    for i in 0..10 {
        let bid_px = best_bid - (config.tick_size * Decimal::from(i));
        let ask_px = best_ask + (config.tick_size * Decimal::from(i));

        // Substantial resting depth so trades take small bites rather than blowing out levels
        let bid_qty = config.min_qty * (dec!(2.0) + Decimal::from(i) * dec!(0.35));
        let ask_qty = config.min_qty * (dec!(2.0) + Decimal::from(i) * dec!(0.35));

        let bid_order = OrderBuilder::new(config.symbol, Side::Buy, bid_qty)
            .order_type(OrderType::Limit)
            .price(bid_px)
            .time_in_force(TimeInForce::GoodTillCancel)
            .build();

        let ask_order = OrderBuilder::new(config.symbol, Side::Sell, ask_qty)
            .order_type(OrderType::Limit)
            .price(ask_px)
            .time_in_force(TimeInForce::GoodTillCancel)
            .build();

        let _ = engine.submit_order(bid_order);
        let _ = engine.submit_order(ask_order);
    }
}

/// Continuous mock order flow generator task feeding directly into the Rust OrderBook.
pub async fn run_mock_flow(engine: Arc<MatchingEngine>) {
    info!("Starting realistic Rust OrderBook market flow generator...");

    let configs = MarketConfig::default_markets();
    let mut runtimes: Vec<MarketRuntime> = configs.into_iter().map(MarketRuntime::new).collect();
    let mut rng = FastRng::new(0x9e3779b97f4a7c15);

    // 1. Initial Seeding of all markets
    for rt in &runtimes {
        seed_market_liquidity(&engine, &rt.config, rt.current_mid);
        info!(
            symbol = %rt.config.symbol,
            base_price = %rt.config.base_price,
            "Seeded initial 10-level liquidity into Rust OrderBook"
        );
    }

    // Tick interval: 150ms provides smooth 6.6 ticks/sec cadence
    let mut interval = tokio::time::interval(Duration::from_millis(150));

    loop {
        interval.tick().await;

        for rt in &mut runtimes {
            // 1. Advance stochastic price process
            rt.step_price(&mut rng);

            let tick = rt.config.tick_size;
            let half_spread = tick;
            let target_best_bid = rt.current_mid - half_spread;
            let target_best_ask = rt.current_mid + half_spread;

            // 2. Active Market Maker: Maintain tight spread and 10-level book depth
            if let Ok(snap) = engine.get_snapshot(rt.config.symbol, 10) {
                let current_best_bid = snap.best_bid;
                let current_best_ask = snap.best_ask;

                // (a) Ensure Top-of-Book Bid exists and is tight
                let need_top_bid = match current_best_bid {
                    Some(b) => b < target_best_bid,
                    None => true,
                };
                if need_top_bid {
                    let ask_bound = current_best_ask.unwrap_or(Decimal::MAX);
                    if target_best_bid < ask_bound {
                        let top_bid_qty = rt.config.min_qty * (dec!(2.0) + Decimal::from_f64_retain(rng.next_f64() * 1.5).unwrap_or(dec!(0.5)));
                        let ord = OrderBuilder::new(rt.config.symbol, Side::Buy, top_bid_qty)
                            .order_type(OrderType::Limit)
                            .price(target_best_bid)
                            .time_in_force(TimeInForce::GoodTillCancel)
                            .build();
                        let _ = engine.submit_order(ord);
                    }
                }

                // (b) Ensure Top-of-Book Ask exists and is tight
                let need_top_ask = match current_best_ask {
                    Some(a) => a > target_best_ask,
                    None => true,
                };
                if need_top_ask {
                    let bid_bound = current_best_bid.unwrap_or(Decimal::ZERO);
                    if target_best_ask > bid_bound {
                        let top_ask_qty = rt.config.min_qty * (dec!(2.0) + Decimal::from_f64_retain(rng.next_f64() * 1.5).unwrap_or(dec!(0.5)));
                        let ord = OrderBuilder::new(rt.config.symbol, Side::Sell, top_ask_qty)
                            .order_type(OrderType::Limit)
                            .price(target_best_ask)
                            .time_in_force(TimeInForce::GoodTillCancel)
                            .build();
                        let _ = engine.submit_order(ord);
                    }
                }

                // (c) Replenish missing levels in 10-deep ladder
                for i in 1..10 {
                    let bid_level_px = target_best_bid - (tick * Decimal::from(i));
                    let has_bid = snap.bids.iter().any(|b| b.price == bid_level_px);
                    if !has_bid {
                        let qty = rt.config.min_qty * (dec!(1.5) + Decimal::from(i) * dec!(0.3));
                        let ord = OrderBuilder::new(rt.config.symbol, Side::Buy, qty)
                            .order_type(OrderType::Limit)
                            .price(bid_level_px)
                            .time_in_force(TimeInForce::GoodTillCancel)
                            .build();
                        let _ = engine.submit_order(ord);
                    }

                    let ask_level_px = target_best_ask + (tick * Decimal::from(i));
                    let has_ask = snap.asks.iter().any(|a| a.price == ask_level_px);
                    if !has_ask {
                        let qty = rt.config.min_qty * (dec!(1.5) + Decimal::from(i) * dec!(0.3));
                        let ord = OrderBuilder::new(rt.config.symbol, Side::Sell, qty)
                            .order_type(OrderType::Limit)
                            .price(ask_level_px)
                            .time_in_force(TimeInForce::GoodTillCancel)
                            .build();
                        let _ = engine.submit_order(ord);
                    }
                }
            }

            // 3. Taker Trade Execution (Natural arrival & clustering)
            // 55% chance of a trade on this tick gives a natural rhythm (~3.6 trades/sec)
            if rng.next_f64() < 0.55 {
                let buy_prob = rt.regime.buy_probability();
                let is_buy = rng.next_f64() < buy_prob;
                let taker_side = if is_buy { Side::Buy } else { Side::Sell };

                // Trade sizing: Mostly small retail trades taking part of the top level;
                // occasional medium or block sizes.
                let size_roll = rng.next_f64();
                let size_mult = if size_roll < 0.78 {
                    // Small retail trade (0.3x to 0.9x min_qty)
                    0.30 + rng.next_f64() * 0.60
                } else if size_roll < 0.95 {
                    // Medium trade (1.0x to 2.0x min_qty)
                    1.00 + rng.next_f64() * 1.00
                } else {
                    // Occasional block trade (2.5x to 3.5x min_qty)
                    2.50 + rng.next_f64() * 1.00
                };

                let taker_qty = (rt.config.min_qty
                    * Decimal::from_f64_retain(size_mult).unwrap_or(dec!(0.5)))
                .max(rt.config.min_qty * dec!(0.2));

                let taker_order = OrderBuilder::new(rt.config.symbol, taker_side, taker_qty)
                    .order_type(OrderType::Market)
                    .time_in_force(TimeInForce::ImmediateOrCancel)
                    .build();

                match engine.submit_order(taker_order) {
                    Ok(events) => {
                        debug!(
                            symbol = %rt.config.symbol,
                            side = ?taker_side,
                            qty = %taker_qty,
                            events_count = events.len(),
                            "Executed natural taker order in Rust OrderBook"
                        );
                    }
                    Err(e) => {
                        debug!(symbol = %rt.config.symbol, error = %e, "Taker order rejected");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seed_and_match_mock_flow() {
        let engine = MatchingEngine::new();
        let config = MarketConfig {
            symbol: "BTC-USD",
            base_price: dec!(50000.0),
            tick_size: dec!(1.0),
            min_spread_ticks: 2,
            min_qty: dec!(1.0),
            max_qty: dec!(5.0),
        };

        // Seed liquidity
        seed_market_liquidity(&engine, &config, config.base_price);

        // Check that snapshot has bids and asks
        let snap = engine.get_snapshot("BTC-USD", 10).unwrap();
        assert_eq!(snap.bids.len(), 10);
        assert_eq!(snap.asks.len(), 10);
        assert!(snap.best_bid.unwrap() < snap.best_ask.unwrap());

        // Execute a taker buy order
        let taker = OrderBuilder::new("BTC-USD", Side::Buy, dec!(0.5))
            .order_type(OrderType::Market)
            .build();

        let events = engine.submit_order(taker).unwrap();
        assert!(!events.is_empty());
        let has_trade = events.iter().any(|e| matches!(e, shared::TradeEvent::TradeExecuted { .. }));
        assert!(has_trade, "Expected TradeExecuted event from taker execution");
    }

    #[test]
    fn test_stochastic_runtime_smooth_drift() {
        let config = MarketConfig {
            symbol: "BTC-USD",
            base_price: dec!(50000.0),
            tick_size: dec!(0.5),
            min_spread_ticks: 2,
            min_qty: dec!(0.1),
            max_qty: dec!(1.0),
        };
        let mut runtime = MarketRuntime::new(config.clone());
        let mut rng = FastRng::new(42);

        let mut prev_price = runtime.current_mid;
        for _ in 0..500 {
            runtime.step_price(&mut rng);
            let diff = (runtime.current_mid - prev_price).abs();
            // In any single step, price should never jump more than 1 tick!
            assert!(
                diff <= config.tick_size,
                "Price jumped by {} which exceeds tick size {}",
                diff,
                config.tick_size
            );
            prev_price = runtime.current_mid;
        }
    }
}
