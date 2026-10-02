//! Core domain types for the order matching engine.
//!
//! Every type here derives `Serialize` / `Deserialize` so it can be sent
//! directly as JSON to a Node.js (server.js) layer or Tauri frontend.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ═══════════════════════════════════════════════════════════════════
// Enums
// ═══════════════════════════════════════════════════════════════════

/// Buy or sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// Returns the opposite side.
    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

/// The kind of order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    /// Resting limit order — fills at the limit price or better, remainder
    /// stays on the book.
    Limit,
    /// Market order — fills at the best available price(s), unfilled
    /// remainder is cancelled.
    Market,
    /// Converts to a `Limit` order when the stop price is reached.
    StopLimit,
    /// Converts to a `Market` order when the stop price is reached.
    StopMarket,
}

impl std::fmt::Display for OrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderType::Limit => write!(f, "LIMIT"),
            OrderType::Market => write!(f, "MARKET"),
            OrderType::StopLimit => write!(f, "STOP_LIMIT"),
            OrderType::StopMarket => write!(f, "STOP_MARKET"),
        }
    }
}

/// Time-in-force policy that controls how long an order stays active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum TimeInForce {
    /// Good-till-cancelled — remains on the book until filled or cancelled.
    #[default]
    GoodTillCancel,
    /// Immediate-or-cancel — fills what it can immediately, the rest is
    /// cancelled.
    ImmediateOrCancel,
    /// Fill-or-kill — must be filled entirely in one shot or rejected.
    FillOrKill,
    /// Good-till-date — remains on the book until the given expiry time.
    GoodTillDate { expiry: DateTime<Utc> },
}


/// Lifecycle status of an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    /// Accepted but not yet matched.
    New,
    /// Partially filled — still resting on the book.
    PartiallyFilled,
    /// Completely filled.
    Filled,
    /// Cancelled by the user or system.
    Cancelled,
    /// Rejected by the engine (e.g. failed validation).
    Rejected,
    /// Expired (GTD).
    Expired,
    /// Stop order waiting for its trigger price.
    PendingTrigger,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderStatus::New => write!(f, "NEW"),
            OrderStatus::PartiallyFilled => write!(f, "PARTIALLY_FILLED"),
            OrderStatus::Filled => write!(f, "FILLED"),
            OrderStatus::Cancelled => write!(f, "CANCELLED"),
            OrderStatus::Rejected => write!(f, "REJECTED"),
            OrderStatus::Expired => write!(f, "EXPIRED"),
            OrderStatus::PendingTrigger => write!(f, "PENDING_TRIGGER"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Order
// ═══════════════════════════════════════════════════════════════════

/// A trading order.
///
/// Create via [`OrderBuilder`] for ergonomic construction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: Uuid,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    /// Limit price. Required for `Limit` and `StopLimit`, ignored for
    /// `Market` and `StopMarket`.
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub time_in_force: TimeInForce,
    /// Trigger price for stop orders.
    pub stop_price: Option<Decimal>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Order {
    /// Quantity that still needs to be filled.
    pub fn remaining_quantity(&self) -> Decimal {
        self.quantity - self.filled_quantity
    }

    /// Returns `true` when `filled_quantity >= quantity`.
    pub fn is_filled(&self) -> bool {
        self.filled_quantity >= self.quantity
    }

    /// Returns `true` if the order is in an active state.
    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            OrderStatus::New | OrderStatus::PartiallyFilled | OrderStatus::PendingTrigger
        )
    }

    /// Record a (partial) fill of `qty` units.
    pub fn fill(&mut self, qty: Decimal) {
        self.filled_quantity += qty;
        self.updated_at = Utc::now();
        if self.is_filled() {
            self.status = OrderStatus::Filled;
        } else {
            self.status = OrderStatus::PartiallyFilled;
        }
    }

    /// Mark the order as cancelled.
    pub fn cancel(&mut self) {
        self.status = OrderStatus::Cancelled;
        self.updated_at = Utc::now();
    }

    /// Mark the order as rejected.
    pub fn reject(&mut self) {
        self.status = OrderStatus::Rejected;
        self.updated_at = Utc::now();
    }
}

// ═══════════════════════════════════════════════════════════════════
// OrderBuilder
// ═══════════════════════════════════════════════════════════════════

/// Fluent builder for [`Order`].
///
/// # Examples
/// ```
/// use shared::types::{OrderBuilder, Side, OrderType, TimeInForce};
/// use rust_decimal_macros::dec;
///
/// let order = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.5))
///     .order_type(OrderType::Limit)
///     .price(dec!(50000))
///     .time_in_force(TimeInForce::GoodTillCancel)
///     .build();
/// ```
pub struct OrderBuilder {
    symbol: String,
    side: Side,
    order_type: OrderType,
    price: Option<Decimal>,
    quantity: Decimal,
    time_in_force: TimeInForce,
    stop_price: Option<Decimal>,
}

impl OrderBuilder {
    /// Start building a new order.
    pub fn new(symbol: impl Into<String>, side: Side, quantity: Decimal) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            order_type: OrderType::Limit,
            price: None,
            quantity,
            time_in_force: TimeInForce::GoodTillCancel,
            stop_price: None,
        }
    }

    pub fn order_type(mut self, order_type: OrderType) -> Self {
        self.order_type = order_type;
        self
    }

    pub fn price(mut self, price: Decimal) -> Self {
        self.price = Some(price);
        self
    }

    pub fn time_in_force(mut self, tif: TimeInForce) -> Self {
        self.time_in_force = tif;
        self
    }

    pub fn stop_price(mut self, stop_price: Decimal) -> Self {
        self.stop_price = Some(stop_price);
        self
    }

    /// Consume the builder and produce an [`Order`].
    pub fn build(self) -> Order {
        let now = Utc::now();
        let status = match self.order_type {
            OrderType::StopLimit | OrderType::StopMarket => OrderStatus::PendingTrigger,
            _ => OrderStatus::New,
        };
        Order {
            id: Uuid::new_v4(),
            symbol: self.symbol,
            side: self.side,
            order_type: self.order_type,
            price: self.price,
            quantity: self.quantity,
            filled_quantity: Decimal::ZERO,
            status,
            time_in_force: self.time_in_force,
            stop_price: self.stop_price,
            created_at: now,
            updated_at: now,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Trade
// ═══════════════════════════════════════════════════════════════════

/// A completed execution (fill) between two orders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: Uuid,
    pub symbol: String,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    /// The execution price (always the maker's price).
    pub price: Decimal,
    pub quantity: Decimal,
    /// Which side was the maker (the resting order).
    pub maker_side: Side,
    pub created_at: DateTime<Utc>,
}

// ═══════════════════════════════════════════════════════════════════
// TradeEvent
// ═══════════════════════════════════════════════════════════════════

/// Events emitted by the matching engine during order processing.
///
/// Serialized with `#[serde(tag = "type")]` so each JSON object carries a
/// `"type"` discriminant — convenient for WebSocket consumers in server.js.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TradeEvent {
    /// Order was accepted into the engine.
    OrderAccepted {
        order: Order,
    },
    /// Order was rejected (failed validation, FOK couldn't fill, etc.).
    OrderRejected {
        order: Order,
        reason: String,
    },
    /// Order was partially filled.
    OrderPartiallyFilled {
        order: Order,
        filled_qty: Decimal,
        fill_price: Decimal,
        trade_id: Uuid,
    },
    /// Order was completely filled.
    OrderFilled {
        order: Order,
        filled_qty: Decimal,
        fill_price: Decimal,
        trade_id: Uuid,
    },
    /// Order was cancelled.
    OrderCancelled {
        order: Order,
        reason: String,
    },
    /// A trade (execution) occurred.
    TradeExecuted {
        trade: Trade,
    },
    /// A stop order was triggered and converted to a regular order.
    StopTriggered {
        original_stop: Order,
        converted_order: Order,
    },
}

// ═══════════════════════════════════════════════════════════════════
// Traits for extensibility
// ═══════════════════════════════════════════════════════════════════

use crate::errors::EngineError;

/// Abstraction over a single-symbol order book.
///
/// The default implementation uses price-time priority (FIFO within each
/// price level, BTreeMap across levels). Implement this trait to plug in
/// alternative matching algorithms (e.g. pro-rata, volume-weighted).
pub trait OrderBookOps: Send {
    /// Process an incoming order: match it and/or place it on the book.
    fn process_order(&mut self, order: &mut Order) -> Vec<TradeEvent>;

    /// Cancel an existing order by its ID.
    fn cancel_order(&mut self, order_id: Uuid) -> Result<TradeEvent, EngineError>;

    /// Best (highest) bid price, if any.
    fn best_bid(&self) -> Option<Decimal>;

    /// Best (lowest) ask price, if any.
    fn best_ask(&self) -> Option<Decimal>;

    /// The symbol this book serves.
    fn symbol(&self) -> &str;

    /// The total number of resting orders.
    fn order_count(&self) -> usize;
}

/// Abstraction over the multi-symbol matching engine.
///
/// Allows swapping the concrete engine implementation without changing
/// callers (e.g. a mock engine for integration tests with server.js).
pub trait MatchingEngineOps: Send + Sync {
    /// Submit a new order for matching.
    fn submit_order(&self, order: Order) -> Result<Vec<TradeEvent>, EngineError>;

    /// Cancel an existing order.
    fn cancel_order(&self, symbol: &str, order_id: Uuid) -> Result<Vec<TradeEvent>, EngineError>;
}

/// Callback interface for trade events.
///
/// Implement this to integrate the matching engine with external systems
/// such as a WebSocket server (server.js), a database logger, or a
/// risk-management module.
pub trait EventHandler: Send + Sync {
    /// Called for every event emitted by the engine.
    fn on_event(&self, event: &TradeEvent);
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn side_opposite() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
    }

    #[test]
    fn order_builder_defaults() {
        let order = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.quantity, dec!(1.0));
        assert_eq!(order.filled_quantity, Decimal::ZERO);
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.time_in_force, TimeInForce::GoodTillCancel);
    }

    #[test]
    fn order_fill_transitions() {
        let mut order = OrderBuilder::new("ETHUSDT", Side::Sell, dec!(10.0))
            .price(dec!(3000))
            .build();

        order.fill(dec!(4.0));
        assert_eq!(order.remaining_quantity(), dec!(6.0));
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert!(!order.is_filled());

        order.fill(dec!(6.0));
        assert_eq!(order.remaining_quantity(), Decimal::ZERO);
        assert_eq!(order.status, OrderStatus::Filled);
        assert!(order.is_filled());
    }

    #[test]
    fn stop_order_starts_pending() {
        let order = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .order_type(OrderType::StopLimit)
            .price(dec!(50000))
            .stop_price(dec!(49000))
            .build();
        assert_eq!(order.status, OrderStatus::PendingTrigger);
    }

    #[test]
    fn trade_event_serializes_with_tag() {
        let order = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();
        let event = TradeEvent::OrderAccepted {
            order: order.clone(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""type":"OrderAccepted"#));
    }

    #[test]
    fn order_cancel_and_reject() {
        let mut order = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
            .price(dec!(50000))
            .build();

        assert!(order.is_active());
        order.cancel();
        assert_eq!(order.status, OrderStatus::Cancelled);
        assert!(!order.is_active());

        let mut order2 = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(dec!(50000))
            .build();
        order2.reject();
        assert_eq!(order2.status, OrderStatus::Rejected);
    }
}
