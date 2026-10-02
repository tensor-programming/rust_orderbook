//! Multi-symbol matching engine orchestrator.
//!
//! Uses `DashMap<String, OrderBook>` for concurrent per-symbol access and
//! `crossbeam::channel` for high-performance trade event broadcasting.

use crossbeam::channel::{self, Receiver, Sender};
use dashmap::DashMap;
use rust_decimal::Decimal;
use tracing::{error, info, warn};
use uuid::Uuid;

use shared::{
    EngineError, EventHandler, MatchingEngineOps, Order, OrderBookOps, OrderType, TradeEvent,
};

use crate::orderbook::OrderBook;
use crate::snapshot::OrderBookSnapshot;
use crate::stop_manager::StopOrderManager;

/// Configuration for the matching engine.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Channel buffer size for trade events.
    pub event_channel_capacity: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            event_channel_capacity: 100_000,
        }
    }
}

/// The main matching engine: routes orders to per-symbol order books,
/// manages stop orders, and publishes trade events.
///
/// # Concurrency
///
/// - `DashMap` provides fine-grained, per-shard locking so different
///   symbols can be matched concurrently.
/// - The engine is `Send + Sync` and safe to share across Tokio tasks
///   or OS threads.
/// - Events are published via a `crossbeam::channel` which supports
///   MPMC (multiple producers, multiple consumers).
///
/// # Extensibility
///
/// - Implements [`MatchingEngineOps`] for trait-based polymorphism.
/// - Supports registering [`EventHandler`] callbacks.
/// - Serializable snapshots for server.js / WebSocket integration.
pub struct MatchingEngine {
    books: DashMap<String, OrderBook>,
    stop_manager: StopOrderManager,
    event_sender: Sender<TradeEvent>,
    event_receiver: Receiver<TradeEvent>,
    event_handlers: Vec<Box<dyn EventHandler>>,
}

impl MatchingEngine {
    /// Create a new matching engine with default configuration.
    ///
    /// Returns `(engine, event_receiver)` — the receiver can be used by
    /// an async task to stream events to server.js via WebSocket.
    pub fn new() -> Self {
        Self::with_config(EngineConfig::default())
    }

    /// Create a new matching engine with custom configuration.
    pub fn with_config(config: EngineConfig) -> Self {
        let (sender, receiver) = channel::bounded(config.event_channel_capacity);
        Self {
            books: DashMap::new(),
            stop_manager: StopOrderManager::new(),
            event_sender: sender,
            event_receiver: receiver,
            event_handlers: Vec::new(),
        }
    }

    /// Get a clone of the event receiver.
    ///
    /// `crossbeam::channel` supports multiple receivers (MPMC), so you
    /// can call this multiple times to create fan-out consumers.
    pub fn event_receiver(&self) -> Receiver<TradeEvent> {
        self.event_receiver.clone()
    }

    /// Register a callback-based event handler.
    pub fn add_event_handler(&mut self, handler: Box<dyn EventHandler>) {
        self.event_handlers.push(handler);
    }

    /// Publish a single event to the channel and all registered handlers.
    fn publish_event(&self, event: &TradeEvent) {
        // Channel (best-effort — don't block if full).
        if let Err(e) = self.event_sender.try_send(event.clone()) {
            warn!("Event channel full or disconnected: {}", e);
        }
        // Callback handlers.
        for handler in &self.event_handlers {
            handler.on_event(event);
        }
    }

    /// Publish a batch of events.
    fn publish_events(&self, events: &[TradeEvent]) {
        for event in events {
            self.publish_event(event);
        }
    }

    // ───────────────────────────────────────────────────────────────
    // Order validation
    // ───────────────────────────────────────────────────────────────

    fn validate_order(order: &Order) -> Result<(), EngineError> {
        if order.symbol.is_empty() {
            return Err(EngineError::InvalidOrder("Symbol must not be empty".into()));
        }
        if order.quantity <= Decimal::ZERO {
            return Err(EngineError::InvalidOrder(
                "Quantity must be positive".into(),
            ));
        }
        match order.order_type {
            OrderType::Limit => {
                if order.price.is_none() {
                    return Err(EngineError::InvalidOrder(
                        "Limit order must have a price".into(),
                    ));
                }
                if let Some(p) = order.price {
                    if p <= Decimal::ZERO {
                        return Err(EngineError::InvalidOrder(
                            "Price must be positive".into(),
                        ));
                    }
                }
            }
            OrderType::StopLimit => {
                if order.price.is_none() {
                    return Err(EngineError::InvalidOrder(
                        "StopLimit order must have a limit price".into(),
                    ));
                }
                if order.stop_price.is_none() {
                    return Err(EngineError::InvalidOrder(
                        "StopLimit order must have a stop price".into(),
                    ));
                }
            }
            OrderType::StopMarket => {
                if order.stop_price.is_none() {
                    return Err(EngineError::InvalidOrder(
                        "StopMarket order must have a stop price".into(),
                    ));
                }
            }
            OrderType::Market => {
                // Market orders with FOK need validation later during matching.
            }
        }
        Ok(())
    }

    // ───────────────────────────────────────────────────────────────
    // Public query API
    // ───────────────────────────────────────────────────────────────

    /// Get a snapshot of the order book for a symbol.
    pub fn get_snapshot(
        &self,
        symbol: &str,
        max_levels: usize,
    ) -> Result<OrderBookSnapshot, EngineError> {
        let book = self
            .books
            .get(symbol)
            .ok_or_else(|| EngineError::BookNotFound(symbol.into()))?;
        Ok(book.snapshot(max_levels))
    }

    /// List all symbols that have an active order book.
    pub fn active_symbols(&self) -> Vec<String> {
        self.books.iter().map(|r| r.key().clone()).collect()
    }

    /// Total number of resting orders across all books.
    pub fn total_order_count(&self) -> usize {
        self.books.iter().map(|r| r.order_count()).sum()
    }

    /// Number of pending stop orders across all symbols.
    pub fn pending_stop_count(&self) -> usize {
        self.stop_manager.total_pending()
    }

    /// Access the stop order manager (e.g. for cancelling stops).
    pub fn stop_manager(&self) -> &StopOrderManager {
        &self.stop_manager
    }
}

impl Default for MatchingEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════
// MatchingEngineOps trait implementation
// ═══════════════════════════════════════════════════════════════════

impl MatchingEngineOps for MatchingEngine {
    fn submit_order(&self, order: Order) -> Result<Vec<TradeEvent>, EngineError> {
        Self::validate_order(&order)?;

        let mut all_events = Vec::new();
        let mut pending_orders = vec![order];

        while let Some(mut current) = pending_orders.pop() {
            // ── Stop orders go to the stop manager ──────────────────
            if matches!(
                current.order_type,
                OrderType::StopLimit | OrderType::StopMarket
            ) {
                match self.stop_manager.add_stop_order(current) {
                    Ok(events) => all_events.extend(events),
                    Err(e) => {
                        error!("Failed to add stop order: {}", e);
                        return Err(e);
                    }
                }
                continue;
            }

            // ── Regular orders go to the order book ─────────────────
            let symbol = current.symbol.clone();
            let events = {
                let mut book = self
                    .books
                    .entry(symbol.clone())
                    .or_insert_with(|| OrderBook::new(symbol.clone()));
                book.process_order(&mut current)
            }; // ← DashMap guard dropped here

            // ── Check for triggered stop orders ─────────────────────
            let last_trade_price = events.iter().rev().find_map(|e| match e {
                TradeEvent::TradeExecuted { trade } => Some(trade.price),
                _ => None,
            });

            if let Some(price) = last_trade_price {
                let triggered = self.stop_manager.check_triggers(&symbol, price);
                for (converted_order, trigger_event) in triggered {
                    all_events.push(trigger_event);
                    pending_orders.push(converted_order);
                }
            }

            all_events.extend(events);
        }

        self.publish_events(&all_events);
        info!(event_count = all_events.len(), "Order processing complete");
        Ok(all_events)
    }

    fn cancel_order(
        &self,
        symbol: &str,
        order_id: Uuid,
    ) -> Result<Vec<TradeEvent>, EngineError> {
        // Try the order book first.
        if let Some(mut book) = self.books.get_mut(symbol) {
            match book.cancel_order(order_id) {
                Ok(event) => {
                    self.publish_event(&event);
                    return Ok(vec![event]);
                }
                Err(EngineError::OrderNotFound(_)) => {
                    // Fall through to try stop orders.
                }
                Err(e) => return Err(e),
            }
        }

        // Try stop orders.
        match self.stop_manager.cancel_stop_order(symbol, order_id) {
            Ok(event) => {
                self.publish_event(&event);
                Ok(vec![event])
            }
            Err(e) => Err(e),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use shared::{OrderBuilder, Side};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn new_engine() -> MatchingEngine {
        MatchingEngine::new()
    }

    #[test]
    fn submit_and_match_limit_orders() {
        let engine = new_engine();

        let sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        engine.submit_order(sell).unwrap();

        let buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        let events = engine.submit_order(buy).unwrap();

        assert!(events
            .iter()
            .any(|e| matches!(e, TradeEvent::TradeExecuted { .. })));
        assert_eq!(engine.total_order_count(), 0);
    }

    #[test]
    fn multi_symbol_isolation() {
        let engine = new_engine();

        // BTCUSDT
        let sell_btc = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        engine.submit_order(sell_btc).unwrap();

        // ETHUSDT
        let sell_eth = OrderBuilder::new("ETHUSDT", Side::Sell, dec!(10.0))
            .price(dec!(3000))
            .build();
        engine.submit_order(sell_eth).unwrap();

        assert_eq!(engine.active_symbols().len(), 2);

        // Buy BTC — should not affect ETH book.
        let buy_btc = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        engine.submit_order(buy_btc).unwrap();

        let btc_snap = engine.get_snapshot("BTCUSDT", 10).unwrap();
        let eth_snap = engine.get_snapshot("ETHUSDT", 10).unwrap();

        assert_eq!(btc_snap.best_ask, None); // filled
        assert_eq!(eth_snap.best_ask, Some(dec!(3000))); // untouched
    }

    #[test]
    fn cancel_via_engine() {
        let engine = new_engine();

        let sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        let sell_id = sell.id;
        engine.submit_order(sell).unwrap();

        let events = engine.cancel_order("BTCUSDT", sell_id).unwrap();
        assert!(events
            .iter()
            .any(|e| matches!(e, TradeEvent::OrderCancelled { .. })));
        assert_eq!(engine.total_order_count(), 0);
    }

    #[test]
    fn stop_limit_triggers_on_trade() {
        let engine = new_engine();

        // Place a sell at 50000.
        let sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        engine.submit_order(sell).unwrap();

        // Place a buy stop-limit: trigger at 50000, limit at 50100.
        let stop = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(0.5))
            .order_type(OrderType::StopLimit)
            .stop_price(dec!(50000))
            .price(dec!(50100))
            .build();
        engine.submit_order(stop).unwrap();
        assert_eq!(engine.pending_stop_count(), 1);

        // Submit a buy that trades at 50000 — this should trigger the stop.
        let buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(0.5))
            .price(dec!(50000))
            .build();
        let events = engine.submit_order(buy).unwrap();

        // Should have the original trade + stop trigger events.
        assert!(events
            .iter()
            .any(|e| matches!(e, TradeEvent::StopTriggered { .. })));
        assert_eq!(engine.pending_stop_count(), 0);
    }

    #[test]
    fn event_channel_receives_events() {
        let engine = new_engine();
        let rx = engine.event_receiver();

        let sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        engine.submit_order(sell).unwrap();

        let buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        engine.submit_order(buy).unwrap();

        // Drain all events from the channel.
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert!(count > 0, "Expected at least one event on the channel");
    }

    #[test]
    fn event_handler_callback() {
        let counter = Arc::new(AtomicUsize::new(0));

        struct CountingHandler {
            counter: Arc<AtomicUsize>,
        }
        impl EventHandler for CountingHandler {
            fn on_event(&self, _event: &TradeEvent) {
                self.counter.fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut engine = new_engine();
        engine.add_event_handler(Box::new(CountingHandler {
            counter: Arc::clone(&counter),
        }));

        let sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        engine.submit_order(sell).unwrap();

        assert!(counter.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn validation_rejects_bad_orders() {
        let engine = new_engine();

        // Empty symbol.
        let bad1 = OrderBuilder::new("", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        assert!(engine.submit_order(bad1).is_err());

        // Zero quantity.
        let bad2 = OrderBuilder::new("BTCUSDT", Side::Buy, Decimal::ZERO)
            .price(dec!(50000))
            .build();
        assert!(engine.submit_order(bad2).is_err());

        // Limit without price.
        let bad3 = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0)).build();
        assert!(engine.submit_order(bad3).is_err());
    }
}
