//! Single-symbol order book with price-time priority matching.
//!
//! ## Data structures
//!
//! - **BTreeMap<Decimal, PriceLevel>** for each side (bids, asks).
//!   - Bids: best bid = highest key → accessed via `.iter().next_back()`.
//!   - Asks: best ask = lowest key  → accessed via `.iter().next()`.
//! - **VecDeque<Order>** inside each `PriceLevel` for FIFO (time priority).
//! - **HashMap<Uuid, (Side, Decimal)>** as an index for O(1) cancel lookups.

use std::collections::{BTreeMap, HashMap, VecDeque};

use chrono::Utc;
use rust_decimal::Decimal;
use tracing::{debug, info, warn};
use uuid::Uuid;

use shared::{
    EngineError, EventHandler, Order, OrderBookOps, OrderType, Side, TimeInForce,
    Trade, TradeEvent,
};

use crate::snapshot::{OrderBookSnapshot, PriceLevelSnapshot};

// ═══════════════════════════════════════════════════════════════════
// PriceLevel
// ═══════════════════════════════════════════════════════════════════

/// A single price level containing orders queued in arrival order.
#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: Decimal,
    pub orders: VecDeque<Order>,
    pub total_quantity: Decimal,
}

impl PriceLevel {
    pub fn new(price: Decimal) -> Self {
        Self {
            price,
            orders: VecDeque::new(),
            total_quantity: Decimal::ZERO,
        }
    }

    pub fn add_order(&mut self, order: Order) {
        self.total_quantity += order.remaining_quantity();
        self.orders.push_back(order);
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }
}

// ═══════════════════════════════════════════════════════════════════
// OrderBook
// ═══════════════════════════════════════════════════════════════════

/// A single-symbol order book using price-time priority.
///
/// This struct is **not** `Sync` — concurrent access across symbols is
/// handled by the `DashMap` in [`crate::engine::MatchingEngine`].
pub struct OrderBook {
    symbol: String,
    bids: BTreeMap<Decimal, PriceLevel>,
    asks: BTreeMap<Decimal, PriceLevel>,
    /// Fast lookup: order_id → (side, price_level_key).
    order_index: HashMap<Uuid, (Side, Decimal)>,
    /// Optional event handlers for extensibility.
    event_handlers: Vec<Box<dyn EventHandler>>,
}

impl OrderBook {
    /// Create a new, empty order book for the given symbol.
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::new(),
            event_handlers: Vec::new(),
        }
    }

    /// Register an event handler that will be called for every event
    /// produced by this book.
    pub fn add_event_handler(&mut self, handler: Box<dyn EventHandler>) {
        self.event_handlers.push(handler);
    }

    /// Notify all registered handlers.
    fn notify(&self, event: &TradeEvent) {
        for handler in &self.event_handlers {
            handler.on_event(event);
        }
    }

    /// Notify handlers for a batch of events.
    fn notify_all(&self, events: &[TradeEvent]) {
        for event in events {
            self.notify(event);
        }
    }

    // ───────────────────────────────────────────────────────────────
    // Read-only accessors
    // ───────────────────────────────────────────────────────────────

    /// Bid-ask spread.
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    /// Number of distinct price levels on the bid side.
    pub fn bid_level_count(&self) -> usize {
        self.bids.len()
    }

    /// Number of distinct price levels on the ask side.
    pub fn ask_level_count(&self) -> usize {
        self.asks.len()
    }

    /// Total quantity across all bid levels.
    pub fn bid_depth(&self) -> Decimal {
        self.bids.values().map(|l| l.total_quantity).sum()
    }

    /// Total quantity across all ask levels.
    pub fn ask_depth(&self) -> Decimal {
        self.asks.values().map(|l| l.total_quantity).sum()
    }

    /// Create an immutable snapshot of the current state.
    pub fn snapshot(&self, max_levels: usize) -> OrderBookSnapshot {
        let bids: Vec<PriceLevelSnapshot> = self
            .bids
            .iter()
            .rev() // highest price first
            .take(max_levels)
            .map(|(price, level)| PriceLevelSnapshot {
                price: *price,
                total_quantity: level.total_quantity,
                order_count: level.order_count(),
            })
            .collect();

        let asks: Vec<PriceLevelSnapshot> = self
            .asks
            .iter() // lowest price first
            .take(max_levels)
            .map(|(price, level)| PriceLevelSnapshot {
                price: *price,
                total_quantity: level.total_quantity,
                order_count: level.order_count(),
            })
            .collect();

        OrderBookSnapshot {
            symbol: self.symbol.clone(),
            best_bid: self.best_bid(),
            best_ask: self.best_ask(),
            spread: self.spread(),
            bid_depth: self.bid_depth(),
            ask_depth: self.ask_depth(),
            bids,
            asks,
            timestamp: Utc::now(),
        }
    }

    // ───────────────────────────────────────────────────────────────
    // Internal matching helpers
    // ───────────────────────────────────────────────────────────────

    /// Check whether `quantity` can be filled entirely at or better than
    /// `price_limit` on the opposing side.  Used by FOK orders.
    fn can_fill_completely(
        &self,
        side: Side,
        quantity: Decimal,
        price_limit: Option<Decimal>,
    ) -> bool {
        let mut remaining = quantity;

        match side {
            // A buy fills against asks (ascending).
            Side::Buy => {
                for (&price, level) in self.asks.iter() {
                    if let Some(limit) = price_limit {
                        if price > limit {
                            break;
                        }
                    }
                    remaining -= level.total_quantity.min(remaining);
                    if remaining <= Decimal::ZERO {
                        return true;
                    }
                }
            }
            // A sell fills against bids (descending).
            Side::Sell => {
                for (&price, level) in self.bids.iter().rev() {
                    if let Some(limit) = price_limit {
                        if price < limit {
                            break;
                        }
                    }
                    remaining -= level.total_quantity.min(remaining);
                    if remaining <= Decimal::ZERO {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Place a resting order on the appropriate side of the book.
    fn place_on_book(&mut self, order: Order) {
        let price = order.price.unwrap_or(Decimal::ZERO);
        let side = order.side;
        let id = order.id;

        let book_side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        book_side
            .entry(price)
            .or_insert_with(|| PriceLevel::new(price))
            .add_order(order);

        self.order_index.insert(id, (side, price));
        debug!(order_id = %id, %side, %price, symbol = %self.symbol, "Order resting on book");
    }
}

// ═══════════════════════════════════════════════════════════════════
// Free function: matching engine core loop
// ═══════════════════════════════════════════════════════════════════
//
// Extracted as a free function so that it can borrow `levels` and
// `order_index` as disjoint fields without fighting the borrow checker.

/// Walk the opposing side of the book and fill `incoming` as far as possible.
///
/// * `levels` — the opposing side's `BTreeMap` (asks for a buy, bids
///   for a sell).
/// * `order_index` — the book-wide order index (for removing filled makers).
/// * `incoming` — the aggressive (taker) order being matched.
/// * `price_limit` — upper bound for buys, lower bound for sells, or `None`
///   for market orders.
/// * `ascending` — `true` when walking asks (lowest first), `false` when
///   walking bids (highest first).
/// * `symbol` — the trading pair symbol.
fn match_against_levels(
    levels: &mut BTreeMap<Decimal, PriceLevel>,
    order_index: &mut HashMap<Uuid, (Side, Decimal)>,
    incoming: &mut Order,
    price_limit: Option<Decimal>,
    ascending: bool,
    symbol: &str,
) -> Vec<TradeEvent> {
    let mut events = Vec::new();

    loop {
        if incoming.remaining_quantity() <= Decimal::ZERO {
            break;
        }

        // Peek at the best opposing price.
        let best_price = if ascending {
            levels.keys().next().copied()
        } else {
            levels.keys().next_back().copied()
        };

        let best_price = match best_price {
            Some(p) => p,
            None => break, // book side is empty
        };

        // Price guard.
        if let Some(limit) = price_limit {
            if ascending && best_price > limit {
                break;
            }
            if !ascending && best_price < limit {
                break;
            }
        }

        // Get the level.
        let level = match levels.get_mut(&best_price) {
            Some(l) => l,
            None => break,
        };

        // Fill orders at this level in FIFO order.
        while !level.orders.is_empty() && incoming.remaining_quantity() > Decimal::ZERO {
            let maker = level.orders.front_mut().unwrap();
            let fill_qty = incoming.remaining_quantity().min(maker.remaining_quantity());
            let fill_price = best_price; // always the maker's price

            // Determine buyer / seller IDs.
            let (buy_order_id, sell_order_id) = match incoming.side {
                Side::Buy => (incoming.id, maker.id),
                Side::Sell => (maker.id, incoming.id),
            };

            let trade = Trade {
                id: Uuid::new_v4(),
                symbol: symbol.to_string(),
                buy_order_id,
                sell_order_id,
                price: fill_price,
                quantity: fill_qty,
                maker_side: maker.side,
                created_at: Utc::now(),
            };

            // Apply fills.
            incoming.fill(fill_qty);
            maker.fill(fill_qty);
            level.total_quantity -= fill_qty;

            let trade_id = trade.id;
            events.push(TradeEvent::TradeExecuted {
                trade: trade.clone(),
            });

            // Maker events.
            if maker.is_filled() {
                let finished_maker = level.orders.pop_front().unwrap();
                order_index.remove(&finished_maker.id);
                events.push(TradeEvent::OrderFilled {
                    order: finished_maker,
                    filled_qty: fill_qty,
                    fill_price,
                    trade_id,
                });
            } else {
                events.push(TradeEvent::OrderPartiallyFilled {
                    order: maker.clone(),
                    filled_qty: fill_qty,
                    fill_price,
                    trade_id,
                });
            }

            // Taker events.
            if incoming.is_filled() {
                events.push(TradeEvent::OrderFilled {
                    order: incoming.clone(),
                    filled_qty: fill_qty,
                    fill_price,
                    trade_id,
                });
            } else {
                events.push(TradeEvent::OrderPartiallyFilled {
                    order: incoming.clone(),
                    filled_qty: fill_qty,
                    fill_price,
                    trade_id,
                });
            }
        }

        // Remove empty level.
        if level.is_empty() {
            levels.remove(&best_price);
        }
    }

    events
}

// ═══════════════════════════════════════════════════════════════════
// OrderBookOps implementation
// ═══════════════════════════════════════════════════════════════════

impl OrderBookOps for OrderBook {
    fn process_order(&mut self, order: &mut Order) -> Vec<TradeEvent> {
        info!(
            order_id = %order.id,
            side = %order.side,
            order_type = %order.order_type,
            price = ?order.price,
            qty = %order.quantity,
            tif = ?order.time_in_force,
            symbol = %self.symbol,
            "Processing order"
        );

        let events = match order.order_type {
            OrderType::Limit => self.process_limit_order(order),
            OrderType::Market => self.process_market_order(order),
            // Stop orders should not reach the book directly — they are
            // held in the StopOrderManager and converted to Limit/Market
            // when triggered.
            OrderType::StopLimit | OrderType::StopMarket => {
                warn!(order_id = %order.id, "Stop order submitted directly to book; rejecting");
                order.reject();
                vec![TradeEvent::OrderRejected {
                    order: order.clone(),
                    reason: "Stop orders must be submitted through the engine".into(),
                }]
            }
        };

        self.notify_all(&events);
        events
    }

    fn cancel_order(&mut self, order_id: Uuid) -> Result<TradeEvent, EngineError> {
        let (side, price) = self
            .order_index
            .remove(&order_id)
            .ok_or(EngineError::OrderNotFound(order_id))?;

        let book_side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        if let Some(level) = book_side.get_mut(&price) {
            if let Some(pos) = level.orders.iter().position(|o| o.id == order_id) {
                let mut cancelled = level.orders.remove(pos).unwrap();
                level.total_quantity -= cancelled.remaining_quantity();
                cancelled.cancel();

                if level.is_empty() {
                    book_side.remove(&price);
                }

                let event = TradeEvent::OrderCancelled {
                    order: cancelled,
                    reason: "Cancelled by user".into(),
                };
                self.notify(&event);
                info!(order_id = %order_id, symbol = %self.symbol, "Order cancelled");
                return Ok(event);
            }
        }

        Err(EngineError::OrderNotFound(order_id))
    }

    fn best_bid(&self) -> Option<Decimal> {
        self.bids.keys().next_back().copied()
    }

    fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn order_count(&self) -> usize {
        self.order_index.len()
    }
}

// ═══════════════════════════════════════════════════════════════════
// Private matching logic
// ═══════════════════════════════════════════════════════════════════

impl OrderBook {
    /// Process a limit order respecting its `TimeInForce` policy.
    fn process_limit_order(&mut self, order: &mut Order) -> Vec<TradeEvent> {
        let mut events = vec![TradeEvent::OrderAccepted {
            order: order.clone(),
        }];

        match order.time_in_force {
            TimeInForce::FillOrKill => {
                if !self.can_fill_completely(order.side, order.quantity, order.price) {
                    order.reject();
                    events.clear();
                    events.push(TradeEvent::OrderRejected {
                        order: order.clone(),
                        reason: "FOK: insufficient liquidity for full fill".into(),
                    });
                    return events;
                }
                // Full fill is possible — execute.
                let fill_events = self.execute_match(order);
                events.extend(fill_events);
            }
            TimeInForce::ImmediateOrCancel => {
                let fill_events = self.execute_match(order);
                events.extend(fill_events);
                // Cancel any unfilled remainder.
                if !order.is_filled() {
                    order.cancel();
                    events.push(TradeEvent::OrderCancelled {
                        order: order.clone(),
                        reason: "IOC: unfilled remainder cancelled".into(),
                    });
                }
            }
            TimeInForce::GoodTillCancel | TimeInForce::GoodTillDate { .. } => {
                let fill_events = self.execute_match(order);
                events.extend(fill_events);
                // Rest unfilled remainder on the book.
                if !order.is_filled() {
                    self.place_on_book(order.clone());
                }
            }
        }

        events
    }

    /// Process a market order.
    fn process_market_order(&mut self, order: &mut Order) -> Vec<TradeEvent> {
        let mut events = vec![TradeEvent::OrderAccepted {
            order: order.clone(),
        }];

        if order.time_in_force == TimeInForce::FillOrKill
            && !self.can_fill_completely(order.side, order.quantity, None) {
                order.reject();
                events.clear();
                events.push(TradeEvent::OrderRejected {
                    order: order.clone(),
                    reason: "FOK Market: insufficient liquidity".into(),
                });
                return events;
            }

        // Market orders have no price limit.
        let fill_events = self.execute_match_no_limit(order);
        events.extend(fill_events);

        // Cancel any unfilled remainder (market orders never rest).
        if !order.is_filled() {
            order.cancel();
            events.push(TradeEvent::OrderCancelled {
                order: order.clone(),
                reason: "Market order: unfilled remainder cancelled".into(),
            });
        }

        events
    }

    /// Execute matching with a price limit (limit orders).
    fn execute_match(&mut self, incoming: &mut Order) -> Vec<TradeEvent> {
        let price_limit = incoming.price;
        let ascending = incoming.side == Side::Buy;
        let symbol = self.symbol.clone();

        let levels = match incoming.side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        match_against_levels(levels, &mut self.order_index, incoming, price_limit, ascending, &symbol)
    }

    /// Execute matching without a price limit (market orders).
    fn execute_match_no_limit(&mut self, incoming: &mut Order) -> Vec<TradeEvent> {
        let ascending = incoming.side == Side::Buy;
        let symbol = self.symbol.clone();

        let levels = match incoming.side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        match_against_levels(levels, &mut self.order_index, incoming, None, ascending, &symbol)
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use shared::{OrderBuilder, OrderStatus};

    fn new_book() -> OrderBook {
        OrderBook::new("BTCUSDT")
    }

    // ── Limit order basics ────────────────────────────────────────

    #[test]
    fn limit_order_rests_when_no_opposing_side() {
        let mut book = new_book();
        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();

        let events = book.process_order(&mut buy);

        // Should have OrderAccepted only (rests on book).
        assert!(events.iter().any(|e| matches!(e, TradeEvent::OrderAccepted { .. })));
        assert_eq!(book.best_bid(), Some(dec!(50000)));
        assert_eq!(book.order_count(), 1);
    }

    #[test]
    fn limit_order_full_fill() {
        let mut book = new_book();

        // Place a sell at 50000.
        let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        book.process_order(&mut sell);

        // Submit a buy at 50000 — should fully match.
        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        let events = book.process_order(&mut buy);

        let trades: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, TradeEvent::TradeExecuted { .. }))
            .collect();
        assert_eq!(trades.len(), 1);

        let fills: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, TradeEvent::OrderFilled { .. }))
            .collect();
        // Both maker and taker filled.
        assert_eq!(fills.len(), 2);

        // Book should be empty.
        assert_eq!(book.order_count(), 0);
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn limit_order_partial_fill() {
        let mut book = new_book();

        // Place a sell of 2.0 at 50000.
        let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(2.0))
            .price(dec!(50000))
            .build();
        book.process_order(&mut sell);

        // Buy 1.0 at 50000 — partial fill of the resting sell.
        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        let events = book.process_order(&mut buy);

        assert!(events.iter().any(|e| matches!(e, TradeEvent::TradeExecuted { .. })));
        // Taker (buy) fully filled, maker (sell) partially filled.
        assert!(events.iter().any(|e| matches!(e, TradeEvent::OrderFilled { .. })));
        assert!(events.iter().any(|e| matches!(e, TradeEvent::OrderPartiallyFilled { .. })));

        // Sell should still rest with remaining 1.0.
        assert_eq!(book.best_ask(), Some(dec!(50000)));
        assert_eq!(book.order_count(), 1);
    }

    // ── Price-time priority ──────────────────────────────────────

    #[test]
    fn price_time_priority() {
        let mut book = new_book();

        // Two sells at the same price — first order should fill first.
        let mut sell1 = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        let sell1_id = sell1.id;
        book.process_order(&mut sell1);

        let mut sell2 = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        book.process_order(&mut sell2);

        // Buy 1.0 — should match sell1 (FIFO).
        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        let events = book.process_order(&mut buy);

        // Find the trade and verify the maker is sell1.
        let trade = events.iter().find_map(|e| match e {
            TradeEvent::TradeExecuted { trade } => Some(trade),
            _ => None,
        });
        assert!(trade.is_some());
        assert_eq!(trade.unwrap().sell_order_id, sell1_id);

        // sell2 should still be resting.
        assert_eq!(book.order_count(), 1);
    }

    #[test]
    fn price_priority_best_price_fills_first() {
        let mut book = new_book();

        // Sell at 50100.
        let mut sell_expensive = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50100))
            .build();
        book.process_order(&mut sell_expensive);

        // Sell at 50000 (better price for buyer).
        let mut sell_cheap = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        let cheap_id = sell_cheap.id;
        book.process_order(&mut sell_cheap);

        // Buy at 50100 — should fill at 50000 first (best ask).
        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50100))
            .build();
        let events = book.process_order(&mut buy);

        let trade = events.iter().find_map(|e| match e {
            TradeEvent::TradeExecuted { trade } => Some(trade),
            _ => None,
        }).unwrap();
        assert_eq!(trade.sell_order_id, cheap_id);
        assert_eq!(trade.price, dec!(50000)); // executed at maker's price
    }

    // ── Market orders ────────────────────────────────────────────

    #[test]
    fn market_order_fills_at_best_available() {
        let mut book = new_book();

        let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(2.0))
            .price(dec!(50000))
            .build();
        book.process_order(&mut sell);

        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .order_type(OrderType::Market)
            .build();
        let events = book.process_order(&mut buy);

        assert!(events.iter().any(|e| matches!(e, TradeEvent::TradeExecuted { .. })));
        assert!(buy.is_filled());
    }

    #[test]
    fn market_order_unfilled_remainder_cancelled() {
        let mut book = new_book();
        // Empty book — no liquidity.
        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .order_type(OrderType::Market)
            .build();
        let events = book.process_order(&mut buy);

        assert!(events.iter().any(|e| matches!(e, TradeEvent::OrderCancelled { .. })));
        assert_eq!(buy.status, OrderStatus::Cancelled);
    }

    // ── IOC orders ───────────────────────────────────────────────

    #[test]
    fn ioc_partial_fill_cancels_remainder() {
        let mut book = new_book();

        let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(0.5))
            .price(dec!(50000))
            .build();
        book.process_order(&mut sell);

        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .time_in_force(TimeInForce::ImmediateOrCancel)
            .build();
        let events = book.process_order(&mut buy);

        // Should fill 0.5 then cancel.
        assert!(events.iter().any(|e| matches!(e, TradeEvent::TradeExecuted { .. })));
        assert!(events.iter().any(|e| matches!(e, TradeEvent::OrderCancelled { .. })));
        assert_eq!(buy.status, OrderStatus::Cancelled);
        assert_eq!(buy.filled_quantity, dec!(0.5));
        assert_eq!(book.order_count(), 0);
    }

    // ── FOK orders ───────────────────────────────────────────────

    #[test]
    fn fok_rejects_when_insufficient_liquidity() {
        let mut book = new_book();

        let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(0.5))
            .price(dec!(50000))
            .build();
        book.process_order(&mut sell);

        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .time_in_force(TimeInForce::FillOrKill)
            .build();
        let events = book.process_order(&mut buy);

        assert!(events.iter().any(|e| matches!(e, TradeEvent::OrderRejected { .. })));
        assert_eq!(buy.status, OrderStatus::Rejected);
        // The resting sell should NOT be touched.
        assert_eq!(book.order_count(), 1);
    }

    #[test]
    fn fok_fills_when_liquidity_sufficient() {
        let mut book = new_book();

        let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        book.process_order(&mut sell);

        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .time_in_force(TimeInForce::FillOrKill)
            .build();
        let events = book.process_order(&mut buy);

        assert!(events.iter().any(|e| matches!(e, TradeEvent::TradeExecuted { .. })));
        assert!(buy.is_filled());
        assert_eq!(book.order_count(), 0);
    }

    // ── Cancellation ─────────────────────────────────────────────

    #[test]
    fn cancel_order_removes_from_book() {
        let mut book = new_book();

        let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        let sell_id = sell.id;
        book.process_order(&mut sell);
        assert_eq!(book.order_count(), 1);

        let event = book.cancel_order(sell_id).unwrap();
        assert!(matches!(event, TradeEvent::OrderCancelled { .. }));
        assert_eq!(book.order_count(), 0);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn cancel_nonexistent_order_errors() {
        let mut book = new_book();
        let result = book.cancel_order(Uuid::new_v4());
        assert!(result.is_err());
    }

    // ── Snapshot ──────────────────────────────────────────────────

    #[test]
    fn snapshot_reflects_book_state() {
        let mut book = new_book();

        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(49900))
            .build();
        book.process_order(&mut buy);

        let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(2.0))
            .price(dec!(50100))
            .build();
        book.process_order(&mut sell);

        let snap = book.snapshot(10);
        assert_eq!(snap.symbol, "BTCUSDT");
        assert_eq!(snap.best_bid, Some(dec!(49900)));
        assert_eq!(snap.best_ask, Some(dec!(50100)));
        assert_eq!(snap.spread, Some(dec!(200)));
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.asks.len(), 1);
    }

    // ── Multi-level matching ─────────────────────────────────────

    #[test]
    fn buy_sweeps_multiple_ask_levels() {
        let mut book = new_book();

        // Asks at 100, 101, 102.
        for price in [dec!(100), dec!(101), dec!(102)] {
            let mut sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
                .price(price)
                .build();
            book.process_order(&mut sell);
        }

        // Buy 2.5 at 102 — should fill 1.0@100, 1.0@101, 0.5@102.
        let mut buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(2.5))
            .price(dec!(102))
            .build();
        let events = book.process_order(&mut buy);

        let trades: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                TradeEvent::TradeExecuted { trade } => Some(trade),
                _ => None,
            })
            .collect();

        assert_eq!(trades.len(), 3);
        assert_eq!(trades[0].price, dec!(100));
        assert_eq!(trades[1].price, dec!(101));
        assert_eq!(trades[2].price, dec!(102));
        assert_eq!(trades[2].quantity, dec!(0.5));

        assert!(buy.is_filled());
        // 0.5 remaining at ask 102.
        assert_eq!(book.best_ask(), Some(dec!(102)));
    }
}
