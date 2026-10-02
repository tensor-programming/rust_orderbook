//! Enriched OrderBook analytics and snapshot generation.

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::types::EnrichedOrderBookSnapshot;
use crate::messages::BookLevel;

/// Analytics and snapshot generator for orderbook depth data.
pub struct OrderBookAnalytics;

impl OrderBookAnalytics {
    /// Generate an enriched orderbook snapshot containing mid-price, micro-price,
    /// spread, spread percentage, and order imbalance metrics.
    pub fn generate_enriched_snapshot(
        symbol: &str,
        market_id: Option<String>,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        max_levels: usize,
    ) -> EnrichedOrderBookSnapshot {
        let trimmed_bids: Vec<BookLevel> = bids.into_iter().take(max_levels).collect();
        let trimmed_asks: Vec<BookLevel> = asks.into_iter().take(max_levels).collect();

        let best_bid = trimmed_bids.first().map(|b| b.price);
        let best_ask = trimmed_asks.first().map(|a| a.price);

        let bid_depth: Decimal = trimmed_bids.iter().map(|b| b.size).sum();
        let ask_depth: Decimal = trimmed_asks.iter().map(|a| a.size).sum();

        let (spread, spread_percentage, mid_price) = match (best_bid, best_ask) {
            (Some(bb), Some(ba)) => {
                let sp = ba - bb;
                let pct = if ba != Decimal::ZERO {
                    (sp / ba) * dec!(100)
                } else {
                    Decimal::ZERO
                };
                let mid = (bb + ba) / Decimal::TWO;
                (Some(sp), Some(pct), Some(mid))
            }
            _ => (None, None, None),
        };

        // Micro-price (volume-weighted top-level mid price):
        // micro_price = (best_bid * ask_size_1 + best_ask * bid_size_1) / (bid_size_1 + ask_size_1)
        let micro_price = match (trimmed_bids.first(), trimmed_asks.first()) {
            (Some(l1_bid), Some(l1_ask)) => {
                let denom = l1_bid.size + l1_ask.size;
                if denom != Decimal::ZERO {
                    let num = (l1_bid.price * l1_ask.size) + (l1_ask.price * l1_bid.size);
                    Some(num / denom)
                } else {
                    None
                }
            }
            _ => None,
        };

        // Order Imbalance: (bid_depth - ask_depth) / (bid_depth + ask_depth)
        let total_depth = bid_depth + ask_depth;
        let order_imbalance = if total_depth != Decimal::ZERO {
            Some((bid_depth - ask_depth) / total_depth)
        } else {
            None
        };

        EnrichedOrderBookSnapshot {
            symbol: symbol.to_string(),
            market_id,
            bids: trimmed_bids,
            asks: trimmed_asks,
            best_bid,
            best_ask,
            spread,
            spread_percentage,
            mid_price,
            micro_price,
            bid_depth,
            ask_depth,
            order_imbalance,
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }
}
