//! Stop order management.
//!
//! Stop orders (StopLimit and StopMarket) are held in a registry and not
//! placed on the order book until their trigger price is reached. When
//! triggered, they are converted to regular Limit or Market orders and
//! resubmitted to the engine.

use dashmap::DashMap;
use rust_decimal::Decimal;
use tracing::info;
use uuid::Uuid;

use shared::{EngineError, Order, OrderStatus, OrderType, Side, TradeEvent};

/// A pending stop order with its trigger condition.
#[derive(Debug, Clone)]
pub struct StopEntry {
    pub order: Order,
    /// The price at which this stop triggers.
    pub trigger_price: Decimal,
}

/// Manages pending stop orders across all symbols.
///
/// Uses `DashMap` for concurrent per-symbol access, consistent with the
/// matching engine's concurrency model.
pub struct StopOrderManager {
    /// symbol → Vec<StopEntry>
    stops: DashMap<String, Vec<StopEntry>>,
}

impl StopOrderManager {
    pub fn new() -> Self {
        Self {
            stops: DashMap::new(),
        }
    }

    /// Add a stop order to the pending registry.
    ///
    /// Returns the events generated (typically an `OrderAccepted` event).
    pub fn add_stop_order(&self, order: Order) -> Result<Vec<TradeEvent>, EngineError> {
        let trigger_price = order
            .stop_price
            .ok_or_else(|| EngineError::InvalidOrder("Stop order missing stop_price".into()))?;

        if !matches!(order.order_type, OrderType::StopLimit | OrderType::StopMarket) {
            return Err(EngineError::InvalidOrder(
                "Only StopLimit and StopMarket orders are accepted".into(),
            ));
        }

        let symbol = order.symbol.clone();
        let events = vec![TradeEvent::OrderAccepted {
            order: order.clone(),
        }];

        self.stops
            .entry(symbol)
            .or_default()
            .push(StopEntry {
                order,
                trigger_price,
            });

        Ok(events)
    }

    /// Cancel a pending stop order by its ID.
    pub fn cancel_stop_order(
        &self,
        symbol: &str,
        order_id: Uuid,
    ) -> Result<TradeEvent, EngineError> {
        let mut entry = self
            .stops
            .get_mut(symbol)
            .ok_or(EngineError::OrderNotFound(order_id))?;

        let pos = entry
            .iter()
            .position(|s| s.order.id == order_id)
            .ok_or(EngineError::OrderNotFound(order_id))?;

        let mut stop = entry.remove(pos);
        stop.order.cancel();

        Ok(TradeEvent::OrderCancelled {
            order: stop.order,
            reason: "Stop order cancelled by user".into(),
        })
    }

    /// Check whether any pending stops for `symbol` should trigger at
    /// the given `last_trade_price`.
    ///
    /// Returns a list of converted orders (Limit or Market) ready to be
    /// submitted to the matching engine.
    pub fn check_triggers(
        &self,
        symbol: &str,
        last_trade_price: Decimal,
    ) -> Vec<(Order, TradeEvent)> {
        let mut triggered = Vec::new();

        if let Some(mut stops) = self.stops.get_mut(symbol) {
            stops.retain(|entry| {
                let should_trigger = match entry.order.side {
                    // Buy stop: triggers when price rises to or above trigger.
                    Side::Buy => last_trade_price >= entry.trigger_price,
                    // Sell stop: triggers when price falls to or below trigger.
                    Side::Sell => last_trade_price <= entry.trigger_price,
                };

                if should_trigger {
                    let mut converted = entry.order.clone();
                    converted.status = OrderStatus::New;

                    match converted.order_type {
                        OrderType::StopLimit => {
                            converted.order_type = OrderType::Limit;
                        }
                        OrderType::StopMarket => {
                            converted.order_type = OrderType::Market;
                            converted.price = None;
                        }
                        _ => {}
                    }

                    let event = TradeEvent::StopTriggered {
                        original_stop: entry.order.clone(),
                        converted_order: converted.clone(),
                    };

                    info!(
                        order_id = %converted.id,
                        trigger_price = %entry.trigger_price,
                        last_trade_price = %last_trade_price,
                        new_type = %converted.order_type,
                        "Stop order triggered"
                    );

                    triggered.push((converted, event));
                    false // remove from pending list
                } else {
                    true // keep
                }
            });
        }

        triggered
    }

    /// Number of pending stop orders for a symbol.
    pub fn pending_count(&self, symbol: &str) -> usize {
        self.stops
            .get(symbol)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Total pending stop orders across all symbols.
    pub fn total_pending(&self) -> usize {
        self.stops.iter().map(|entry| entry.value().len()).sum()
    }
}

impl Default for StopOrderManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use shared::OrderBuilder;

    #[test]
    fn add_and_trigger_buy_stop() {
        let mgr = StopOrderManager::new();

        let order = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .order_type(OrderType::StopLimit)
            .price(dec!(51000))
            .stop_price(dec!(50500))
            .build();

        mgr.add_stop_order(order).unwrap();
        assert_eq!(mgr.pending_count("BTCUSDT"), 1);

        // Price below trigger — should not trigger.
        let triggered = mgr.check_triggers("BTCUSDT", dec!(50000));
        assert!(triggered.is_empty());
        assert_eq!(mgr.pending_count("BTCUSDT"), 1);

        // Price at trigger — should trigger.
        let triggered = mgr.check_triggers("BTCUSDT", dec!(50500));
        assert_eq!(triggered.len(), 1);
        let (converted, _event) = &triggered[0];
        assert_eq!(converted.order_type, OrderType::Limit);
        assert_eq!(converted.price, Some(dec!(51000)));
        assert_eq!(converted.status, OrderStatus::New);
        assert_eq!(mgr.pending_count("BTCUSDT"), 0);
    }

    #[test]
    fn add_and_trigger_sell_stop_market() {
        let mgr = StopOrderManager::new();

        let order = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(0.5))
            .order_type(OrderType::StopMarket)
            .stop_price(dec!(48000))
            .build();

        mgr.add_stop_order(order).unwrap();

        // Price above trigger — no trigger.
        let triggered = mgr.check_triggers("BTCUSDT", dec!(49000));
        assert!(triggered.is_empty());

        // Price at trigger.
        let triggered = mgr.check_triggers("BTCUSDT", dec!(48000));
        assert_eq!(triggered.len(), 1);
        let (converted, _) = &triggered[0];
        assert_eq!(converted.order_type, OrderType::Market);
        assert_eq!(converted.price, None);
    }

    #[test]
    fn cancel_stop_order() {
        let mgr = StopOrderManager::new();

        let order = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .order_type(OrderType::StopLimit)
            .price(dec!(51000))
            .stop_price(dec!(50500))
            .build();
        let order_id = order.id;

        mgr.add_stop_order(order).unwrap();
        assert_eq!(mgr.pending_count("BTCUSDT"), 1);

        let event = mgr.cancel_stop_order("BTCUSDT", order_id).unwrap();
        assert!(matches!(event, TradeEvent::OrderCancelled { .. }));
        assert_eq!(mgr.pending_count("BTCUSDT"), 0);
    }

    #[test]
    fn reject_non_stop_order() {
        let mgr = StopOrderManager::new();
        let order = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .order_type(OrderType::Limit)
            .price(dec!(50000))
            .build();
        let result = mgr.add_stop_order(order);
        assert!(result.is_err());
    }
}
