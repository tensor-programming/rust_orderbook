//! Conversions between internal domain types and generated protobuf types.

use std::str::FromStr;

use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use shared::{EngineError, Order, OrderStatus, OrderType, Side, TimeInForce, Trade, TradeEvent};

use crate::snapshot::{OrderBookSnapshot, PriceLevelSnapshot};

pub mod proto {
    tonic::include_proto!("order_matching");
}

// ═══════════════════════════════════════════════════════════════════
// Enum Conversions
// ═══════════════════════════════════════════════════════════════════

impl From<Side> for proto::Side {
    fn from(s: Side) -> Self {
        match s {
            Side::Buy => proto::Side::Buy,
            Side::Sell => proto::Side::Sell,
        }
    }
}

impl TryFrom<proto::Side> for Side {
    type Error = EngineError;

    fn try_from(s: proto::Side) -> Result<Self, Self::Error> {
        match s {
            proto::Side::Buy => Ok(Side::Buy),
            proto::Side::Sell => Ok(Side::Sell),
            proto::Side::Unspecified => Err(EngineError::InvalidOrder(
                "Order side cannot be unspecified".to_string(),
            )),
        }
    }
}

impl From<OrderType> for proto::OrderType {
    fn from(t: OrderType) -> Self {
        match t {
            OrderType::Limit => proto::OrderType::Limit,
            OrderType::Market => proto::OrderType::Market,
            OrderType::StopLimit => proto::OrderType::StopLimit,
            OrderType::StopMarket => proto::OrderType::StopMarket,
        }
    }
}

impl TryFrom<proto::OrderType> for OrderType {
    type Error = EngineError;

    fn try_from(t: proto::OrderType) -> Result<Self, Self::Error> {
        match t {
            proto::OrderType::Limit => Ok(OrderType::Limit),
            proto::OrderType::Market => Ok(OrderType::Market),
            proto::OrderType::StopLimit => Ok(OrderType::StopLimit),
            proto::OrderType::StopMarket => Ok(OrderType::StopMarket),
            proto::OrderType::Unspecified => Err(EngineError::InvalidOrder(
                "Order type cannot be unspecified".to_string(),
            )),
        }
    }
}

impl From<TimeInForce> for proto::TimeInForce {
    fn from(tif: TimeInForce) -> Self {
        match tif {
            TimeInForce::GoodTillCancel => proto::TimeInForce::GoodTillCancel,
            TimeInForce::ImmediateOrCancel => proto::TimeInForce::ImmediateOrCancel,
            TimeInForce::FillOrKill => proto::TimeInForce::FillOrKill,
            TimeInForce::GoodTillDate { .. } => proto::TimeInForce::GoodTillDate,
        }
    }
}

impl From<OrderStatus> for proto::OrderStatus {
    fn from(s: OrderStatus) -> Self {
        match s {
            OrderStatus::New => proto::OrderStatus::New,
            OrderStatus::PartiallyFilled => proto::OrderStatus::PartiallyFilled,
            OrderStatus::Filled => proto::OrderStatus::Filled,
            OrderStatus::Cancelled => proto::OrderStatus::Cancelled,
            OrderStatus::Rejected => proto::OrderStatus::Rejected,
            OrderStatus::Expired => proto::OrderStatus::Expired,
            OrderStatus::PendingTrigger => proto::OrderStatus::PendingTrigger,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Message Conversions
// ═══════════════════════════════════════════════════════════════════

impl From<&Order> for proto::OrderMsg {
    fn from(o: &Order) -> Self {
        let expire_at_unix_ms = match o.time_in_force {
            TimeInForce::GoodTillDate { expiry } => Some(expiry.timestamp_millis()),
            _ => None,
        };

        proto::OrderMsg {
            id: o.id.to_string(),
            symbol: o.symbol.clone(),
            side: proto::Side::from(o.side) as i32,
            order_type: proto::OrderType::from(o.order_type) as i32,
            price: o.price.map(|p| p.to_string()),
            quantity: o.quantity.to_string(),
            filled_quantity: o.filled_quantity.to_string(),
            status: proto::OrderStatus::from(o.status) as i32,
            time_in_force: proto::TimeInForce::from(o.time_in_force) as i32,
            stop_price: o.stop_price.map(|p| p.to_string()),
            created_at_unix_ms: o.created_at.timestamp_millis(),
            updated_at_unix_ms: o.updated_at.timestamp_millis(),
            expire_at_unix_ms,
        }
    }
}

impl From<&Trade> for proto::TradeMsg {
    fn from(t: &Trade) -> Self {
        proto::TradeMsg {
            id: t.id.to_string(),
            symbol: t.symbol.clone(),
            buy_order_id: t.buy_order_id.to_string(),
            sell_order_id: t.sell_order_id.to_string(),
            price: t.price.to_string(),
            quantity: t.quantity.to_string(),
            maker_side: proto::Side::from(t.maker_side) as i32,
            created_at_unix_ms: t.created_at.timestamp_millis(),
        }
    }
}

impl From<&PriceLevelSnapshot> for proto::PriceLevelMsg {
    fn from(level: &PriceLevelSnapshot) -> Self {
        proto::PriceLevelMsg {
            price: level.price.to_string(),
            total_quantity: level.total_quantity.to_string(),
            order_count: level.order_count as u64,
        }
    }
}

impl From<&OrderBookSnapshot> for proto::OrderBookSnapshotMsg {
    fn from(snap: &OrderBookSnapshot) -> Self {
        proto::OrderBookSnapshotMsg {
            symbol: snap.symbol.clone(),
            bids: snap.bids.iter().map(proto::PriceLevelMsg::from).collect(),
            asks: snap.asks.iter().map(proto::PriceLevelMsg::from).collect(),
            best_bid: snap.best_bid.map(|p| p.to_string()),
            best_ask: snap.best_ask.map(|p| p.to_string()),
            spread: snap.spread.map(|p| p.to_string()),
            bid_depth: snap.bid_depth.to_string(),
            ask_depth: snap.ask_depth.to_string(),
            timestamp_unix_ms: snap.timestamp.timestamp_millis(),
        }
    }
}

impl From<&TradeEvent> for proto::TradeEventMsg {
    fn from(event: &TradeEvent) -> Self {
        let event_variant = match event {
            TradeEvent::OrderAccepted { order } => {
                proto::trade_event_msg::Event::OrderAccepted(proto::OrderAcceptedEvent {
                    order: Some(proto::OrderMsg::from(order)),
                })
            }
            TradeEvent::OrderRejected { order, reason } => {
                proto::trade_event_msg::Event::OrderRejected(proto::OrderRejectedEvent {
                    order: Some(proto::OrderMsg::from(order)),
                    reason: reason.clone(),
                })
            }
            TradeEvent::OrderPartiallyFilled {
                order,
                filled_qty,
                fill_price,
                trade_id,
            } => proto::trade_event_msg::Event::OrderPartiallyFilled(
                proto::OrderPartiallyFilledEvent {
                    order: Some(proto::OrderMsg::from(order)),
                    filled_qty: filled_qty.to_string(),
                    fill_price: fill_price.to_string(),
                    trade_id: trade_id.to_string(),
                },
            ),
            TradeEvent::OrderFilled {
                order,
                filled_qty,
                fill_price,
                trade_id,
            } => proto::trade_event_msg::Event::OrderFilled(proto::OrderFilledEvent {
                order: Some(proto::OrderMsg::from(order)),
                filled_qty: filled_qty.to_string(),
                fill_price: fill_price.to_string(),
                trade_id: trade_id.to_string(),
            }),
            TradeEvent::OrderCancelled { order, reason } => {
                proto::trade_event_msg::Event::OrderCancelled(proto::OrderCancelledEvent {
                    order: Some(proto::OrderMsg::from(order)),
                    reason: reason.clone(),
                })
            }
            TradeEvent::TradeExecuted { trade } => {
                proto::trade_event_msg::Event::TradeExecuted(proto::TradeExecutedEvent {
                    trade: Some(proto::TradeMsg::from(trade)),
                })
            }
            TradeEvent::StopTriggered {
                original_stop,
                converted_order,
            } => proto::trade_event_msg::Event::StopTriggered(proto::StopTriggeredEvent {
                original_stop: Some(proto::OrderMsg::from(original_stop)),
                converted_order: Some(proto::OrderMsg::from(converted_order)),
            }),
        };

        proto::TradeEventMsg {
            event: Some(event_variant),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Request Parsing Helpers
// ═══════════════════════════════════════════════════════════════════

impl proto::PlaceOrderRequest {
    /// Convert the gRPC `PlaceOrderRequest` into an internal domain `Order`.
    pub fn into_order(self) -> Result<Order, EngineError> {
        let side_enum = proto::Side::try_from(self.side)
            .map_err(|_| EngineError::InvalidOrder(format!("Invalid side value: {}", self.side)))?;
        let side = Side::try_from(side_enum)?;

        let type_enum = proto::OrderType::try_from(self.order_type).map_err(|_| {
            EngineError::InvalidOrder(format!("Invalid order_type value: {}", self.order_type))
        })?;
        let order_type = OrderType::try_from(type_enum)?;

        let quantity = Decimal::from_str(&self.quantity).map_err(|e| {
            EngineError::InvalidOrder(format!("Invalid quantity '{}': {}", self.quantity, e))
        })?;

        let price =
            match self.price {
                Some(ref p) if !p.trim().is_empty() => Some(Decimal::from_str(p).map_err(|e| {
                    EngineError::InvalidOrder(format!("Invalid price '{}': {}", p, e))
                })?),
                _ => None,
            };

        let stop_price = match self.stop_price {
            Some(ref p) if !p.trim().is_empty() => Some(Decimal::from_str(p).map_err(|e| {
                EngineError::InvalidOrder(format!("Invalid stop_price '{}': {}", p, e))
            })?),
            _ => None,
        };

        let tif_enum = proto::TimeInForce::try_from(self.time_in_force)
            .unwrap_or(proto::TimeInForce::GoodTillCancel);
        let time_in_force = match tif_enum {
            proto::TimeInForce::ImmediateOrCancel => TimeInForce::ImmediateOrCancel,
            proto::TimeInForce::FillOrKill => TimeInForce::FillOrKill,
            proto::TimeInForce::GoodTillDate => {
                let expiry = self
                    .expire_at_unix_ms
                    .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                    .ok_or_else(|| {
                        EngineError::InvalidOrder(
                            "GoodTillDate order requires valid expire_at_unix_ms timestamp"
                                .to_string(),
                        )
                    })?;
                TimeInForce::GoodTillDate { expiry }
            }
            _ => TimeInForce::GoodTillCancel,
        };

        let id = match self.custom_order_id {
            Some(ref id_str) if !id_str.trim().is_empty() => {
                Uuid::parse_str(id_str).map_err(|e| {
                    EngineError::InvalidOrder(format!(
                        "Invalid custom_order_id UUID '{}': {}",
                        id_str, e
                    ))
                })?
            }
            _ => Uuid::new_v4(),
        };

        let now = Utc::now();
        let status = match order_type {
            OrderType::StopLimit | OrderType::StopMarket => OrderStatus::PendingTrigger,
            _ => OrderStatus::New,
        };

        Ok(Order {
            id,
            symbol: self.symbol,
            side,
            order_type,
            price,
            quantity,
            filled_quantity: Decimal::ZERO,
            status,
            time_in_force,
            stop_price,
            created_at: now,
            updated_at: now,
        })
    }
}
