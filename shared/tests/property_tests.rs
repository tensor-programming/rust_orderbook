//! Property-based tests for shared domain types and order invariants.

use proptest::prelude::*;
use rust_decimal::Decimal;
use shared::{Order, OrderBuilder, OrderStatus, OrderType, Side, TimeInForce, Trade, TradeEvent};
use uuid::Uuid;

prop_compose! {
    fn arb_decimal(min: i64, max: i64)(val in min..=max, scale in 0u32..=4u32) -> Decimal {
        Decimal::new(val, scale)
    }
}

prop_compose! {
    fn arb_side()(is_buy in any::<bool>()) -> Side {
        if is_buy { Side::Buy } else { Side::Sell }
    }
}

prop_compose! {
    fn arb_order_type()(kind in 0..4) -> OrderType {
        match kind {
            0 => OrderType::Limit,
            1 => OrderType::Market,
            2 => OrderType::StopLimit,
            _ => OrderType::StopMarket,
        }
    }
}

prop_compose! {
    fn arb_order()(
        symbol in "[A-Z]{3,6}",
        side in arb_side(),
        order_type in arb_order_type(),
        price_val in 1i64..100_000,
        qty_val in 1i64..10_000,
    ) -> Order {
        let price = Decimal::new(price_val, 2);
        let qty = Decimal::new(qty_val, 2);
        OrderBuilder::new(symbol, side, qty)
            .order_type(order_type)
            .price(price)
            .time_in_force(TimeInForce::GoodTillCancel)
            .build()
    }
}

proptest! {
    #[test]
    fn prop_side_involution(side in arb_side()) {
        prop_assert_eq!(side.opposite().opposite(), side);
        prop_assert_ne!(side, side.opposite());
    }

    #[test]
    fn prop_order_fill_conservation(
        mut order in arb_order(),
        fill_fraction in 1u32..100u32
    ) {
        let initial_qty = order.quantity;
        let fill_amount = initial_qty * Decimal::new(fill_fraction as i64, 2);

        if fill_amount <= initial_qty && fill_amount > Decimal::ZERO {
            order.fill(fill_amount);

            // Invariant 1: filled + remaining == total quantity
            prop_assert_eq!(order.filled_quantity + order.remaining_quantity(), initial_qty);

            // Invariant 2: status matches completion
            if order.filled_quantity >= initial_qty {
                prop_assert_eq!(order.status, OrderStatus::Filled);
                prop_assert!(order.is_filled());
            } else {
                prop_assert_eq!(order.status, OrderStatus::PartiallyFilled);
                prop_assert!(!order.is_filled());
            }
        }
    }

    #[test]
    fn prop_order_json_roundtrip(order in arb_order()) {
        let json = serde_json::to_string(&order).expect("Serialization failed");
        let deserialized: Order = serde_json::from_str(&json).expect("Deserialization failed");

        prop_assert_eq!(order.id, deserialized.id);
        prop_assert_eq!(order.symbol, deserialized.symbol);
        prop_assert_eq!(order.side, deserialized.side);
        prop_assert_eq!(order.order_type, deserialized.order_type);
        prop_assert_eq!(order.price, deserialized.price);
        prop_assert_eq!(order.quantity, deserialized.quantity);
        prop_assert_eq!(order.status, deserialized.status);
    }

    #[test]
    fn prop_trade_json_roundtrip(
        symbol in "[A-Z]{3,6}",
        price_val in 1i64..100_000,
        qty_val in 1i64..10_000,
        side in arb_side()
    ) {
        let trade = Trade {
            id: Uuid::new_v4(),
            symbol,
            buy_order_id: Uuid::new_v4(),
            sell_order_id: Uuid::new_v4(),
            price: Decimal::new(price_val, 2),
            quantity: Decimal::new(qty_val, 2),
            maker_side: side,
            created_at: chrono::Utc::now(),
        };

        let json = serde_json::to_string(&trade).expect("Serialization failed");
        let deserialized: Trade = serde_json::from_str(&json).expect("Deserialization failed");

        prop_assert_eq!(trade.id, deserialized.id);
        prop_assert_eq!(trade.price, deserialized.price);
        prop_assert_eq!(trade.quantity, deserialized.quantity);
        prop_assert_eq!(trade.maker_side, deserialized.maker_side);
    }

    #[test]
    fn prop_trade_event_tagged_serialization(order in arb_order()) {
        let event = TradeEvent::OrderAccepted { order: order.clone() };
        let json = serde_json::to_string(&event).expect("Serialization failed");
        let deserialized: TradeEvent = serde_json::from_str(&json).expect("Deserialization failed");

        match deserialized {
            TradeEvent::OrderAccepted { order: deserialized_order } => {
                prop_assert_eq!(order.id, deserialized_order.id);
            }
            _ => prop_assert!(false, "Expected OrderAccepted variant"),
        }
    }
}
