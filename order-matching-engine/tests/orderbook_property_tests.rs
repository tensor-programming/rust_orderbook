//! Property-based testing for OrderBook invariants.

use order_matching_engine::OrderBook;
use proptest::collection::vec;
use proptest::prelude::*;
use rust_decimal::Decimal;
use shared::{OrderBookOps, OrderBuilder, OrderType, Side, TimeInForce, TradeEvent};

#[derive(Debug, Clone)]
struct FuzzOrderSpec {
    side: Side,
    order_type: OrderType,
    price: i64,
    qty: i64,
    tif: TimeInForce,
}

prop_compose! {
    fn arb_side()(is_buy in any::<bool>()) -> Side {
        if is_buy { Side::Buy } else { Side::Sell }
    }
}

prop_compose! {
    fn arb_fuzz_order()(
        side in arb_side(),
        is_market in any::<bool>(),
        price in 100i64..500i64,
        qty in 1i64..50i64,
        tif_code in 0..3
    ) -> FuzzOrderSpec {
        let order_type = if is_market { OrderType::Market } else { OrderType::Limit };
        let tif = match tif_code {
            0 => TimeInForce::GoodTillCancel,
            1 => TimeInForce::ImmediateOrCancel,
            _ => TimeInForce::FillOrKill,
        };
        FuzzOrderSpec {
            side,
            order_type,
            price,
            qty,
            tif,
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn prop_no_crossed_orders_rest_on_book(orders in vec(arb_fuzz_order(), 1..60)) {
        let mut book = OrderBook::new("BTCUSDT");

        for spec in orders {
            let price = Decimal::new(spec.price, 1);
            let qty = Decimal::new(spec.qty, 1);

            let mut order = OrderBuilder::new("BTCUSDT", spec.side, qty)
                .order_type(spec.order_type)
                .price(price)
                .time_in_force(spec.tif)
                .build();

            let _events = book.process_order(&mut order);

            // Invariant 1: Resting Spread is always positive (No crossed book)
            if let (Some(best_bid), Some(best_ask)) = (book.best_bid(), book.best_ask()) {
                prop_assert!(
                    best_bid < best_ask,
                    "Order book crossed! best_bid={} >= best_ask={}",
                    best_bid,
                    best_ask
                );
            }

            // Invariant 2: Depths must be non-negative
            prop_assert!(book.bid_depth() >= Decimal::ZERO);
            prop_assert!(book.ask_depth() >= Decimal::ZERO);
        }
    }

    #[test]
    fn prop_trade_volume_conservation(
        bids in vec(100i64..200i64, 1..15),
        asks in vec(150i64..250i64, 1..15)
    ) {
        let mut book = OrderBook::new("ETHUSDT");

        // Seed resting asks
        for (_, &price_int) in asks.iter().enumerate() {
            let price = Decimal::new(price_int, 0);
            let mut sell = OrderBuilder::new("ETHUSDT", Side::Sell, Decimal::ONE)
                .price(price)
                .build();
            book.process_order(&mut sell);
        }

        // Send bids
        for &price_int in &bids {
            let price = Decimal::new(price_int, 0);
            let mut buy = OrderBuilder::new("ETHUSDT", Side::Buy, Decimal::ONE)
                .price(price)
                .build();
            let events = book.process_order(&mut buy);

            // Verify trade volume consistency
            for ev in &events {
                if let TradeEvent::TradeExecuted { trade } = ev {
                    prop_assert!(trade.price > Decimal::ZERO);
                    prop_assert!(trade.quantity > Decimal::ZERO);
                    prop_assert!(trade.quantity <= Decimal::ONE);
                }
            }
        }
    }
}
