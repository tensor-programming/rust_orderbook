//! Point-in-time snapshots of an order book.
//!
//! Snapshots are cheap, immutable, serializable captures of the book's
//! state — ideal for sending to a frontend via WebSocket or REST.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A single aggregated price level in a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevelSnapshot {
    pub price: Decimal,
    pub total_quantity: Decimal,
    pub order_count: usize,
}

/// An immutable point-in-time snapshot of an order book.
///
/// All fields are `Serialize` + `Deserialize` so the snapshot can be
/// sent directly as JSON to server.js or the Tauri frontend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub symbol: String,
    pub bids: Vec<PriceLevelSnapshot>,
    pub asks: Vec<PriceLevelSnapshot>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub bid_depth: Decimal,
    pub ask_depth: Decimal,
    pub timestamp: DateTime<Utc>,
}

impl OrderBookSnapshot {
    /// Mid-price — average of best bid and best ask.
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::TWO),
            _ => None,
        }
    }

    /// Total number of resting orders across all levels.
    pub fn total_order_count(&self) -> usize {
        self.bids.iter().map(|l| l.order_count).sum::<usize>()
            + self.asks.iter().map(|l| l.order_count).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn mid_price_calculation() {
        let snap = OrderBookSnapshot {
            symbol: "BTCUSDT".into(),
            bids: vec![],
            asks: vec![],
            best_bid: Some(dec!(49990)),
            best_ask: Some(dec!(50010)),
            spread: Some(dec!(20)),
            bid_depth: Decimal::ZERO,
            ask_depth: Decimal::ZERO,
            timestamp: Utc::now(),
        };
        assert_eq!(snap.mid_price(), Some(dec!(50000)));
    }

    #[test]
    fn snapshot_serializes_to_json() {
        let snap = OrderBookSnapshot {
            symbol: "ETHUSDT".into(),
            bids: vec![PriceLevelSnapshot {
                price: dec!(3000),
                total_quantity: dec!(5.0),
                order_count: 2,
            }],
            asks: vec![],
            best_bid: Some(dec!(3000)),
            best_ask: None,
            spread: None,
            bid_depth: dec!(5.0),
            ask_depth: Decimal::ZERO,
            timestamp: Utc::now(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("ETHUSDT"));
        assert!(json.contains("3000"));
    }
}
