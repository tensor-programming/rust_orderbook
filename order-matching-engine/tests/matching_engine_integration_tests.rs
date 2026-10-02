//! Integration tests for the Order Matching Engine.

use std::sync::Arc;

use order_matching_engine::grpc::proto::{
    self, CancelOrderRequest, GetOrderBookRequest, PlaceOrderRequest,
};
use order_matching_engine::{MatchingEngine, OrderMatchingService, OrderMatchingServiceImpl};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use shared::{MatchingEngineOps, OrderBuilder, OrderType, Side, TimeInForce, TradeEvent};
use tonic::Request;

#[tokio::test]
async fn test_deep_order_book_sweep_and_partial_fills() {
    let engine = MatchingEngine::new();

    // Setup ask depth: 10 levels of 1.0 BTC each from 50010 to 50100
    for i in 1..=10 {
        let price = Decimal::new(50000 + i * 10, 0);
        let sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
            .price(price)
            .build();
        engine.submit_order(sell).unwrap();
    }

    assert_eq!(engine.total_order_count(), 10);
    let snap_before = engine.get_snapshot("BTCUSDT", 20).unwrap();
    assert_eq!(snap_before.ask_depth, dec!(10.0));
    assert_eq!(snap_before.best_ask, Some(dec!(50010)));

    // Aggressive market taker sweeps 5.5 BTC
    let buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(5.5))
        .order_type(OrderType::Market)
        .build();

    let events = engine.submit_order(buy).unwrap();

    let executed_trades: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TradeEvent::TradeExecuted { trade } => Some(trade),
            _ => None,
        })
        .collect();

    // Should fill 5 full levels (1.0 each) + 1 partial level (0.5) = 6 trades
    assert_eq!(executed_trades.len(), 6);
    let total_traded: Decimal = executed_trades.iter().map(|t| t.quantity).sum();
    assert_eq!(total_traded, dec!(5.5));

    // Verify remaining depth on the book
    let snap_after = engine.get_snapshot("BTCUSDT", 20).unwrap();
    assert_eq!(snap_after.ask_depth, dec!(4.5));
    assert_eq!(snap_after.best_ask, Some(dec!(50060))); // Level 6 still has 0.5 resting
}

#[tokio::test]
async fn test_concurrent_multi_symbol_matching() {
    let engine = Arc::new(MatchingEngine::new());
    let mut handles = Vec::new();

    let symbols = vec!["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT"];

    for symbol in symbols {
        let eng = Arc::clone(&engine);
        let sym = symbol.to_string();

        let handle = tokio::spawn(async move {
            // Place 50 resting sell orders
            for i in 1..=50 {
                let price = Decimal::new(1000 + i * 10, 0);
                let sell = OrderBuilder::new(&sym, Side::Sell, dec!(1.0))
                    .price(price)
                    .build();
                eng.submit_order(sell).unwrap();
            }

            // Place 25 crossing buy orders
            for i in 1..=25 {
                let price = Decimal::new(1000 + i * 10, 0);
                let buy = OrderBuilder::new(&sym, Side::Buy, dec!(1.0))
                    .price(price)
                    .build();
                let events = eng.submit_order(buy).unwrap();
                assert!(events.iter().any(|e| matches!(e, TradeEvent::TradeExecuted { .. })));
            }
        });

        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(engine.active_symbols().len(), 4);
    for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT"] {
        let snap = engine.get_snapshot(sym, 100).unwrap();
        assert_eq!(snap.asks.len(), 25); // 25 remaining resting asks
    }
}

#[tokio::test]
async fn test_stop_order_trigger_chain() {
    let engine = MatchingEngine::new();

    // Resting order at 50000
    let initial_sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(1.0))
        .price(dec!(50000))
        .build();
    engine.submit_order(initial_sell).unwrap();

    // Stop 1: StopLimit Buy (triggers at 50000, limit at 50100)
    let stop_limit = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
        .order_type(OrderType::StopLimit)
        .stop_price(dec!(50000))
        .price(dec!(50100))
        .build();
    engine.submit_order(stop_limit).unwrap();

    assert_eq!(engine.pending_stop_count(), 1);

    // Crossing order triggers Stop 1
    let taker_buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(1.0))
        .price(dec!(50000))
        .build();

    let events = engine.submit_order(taker_buy).unwrap();

    // Verify stop order was triggered
    assert!(events.iter().any(|e| matches!(e, TradeEvent::StopTriggered { .. })));
    assert_eq!(engine.pending_stop_count(), 0);

    // Converted order should now be resting on the bid side at 50100
    let snap = engine.get_snapshot("BTCUSDT", 10).unwrap();
    assert_eq!(snap.best_bid, Some(dec!(50100)));
}

#[tokio::test]
async fn test_fok_and_ioc_edge_cases() {
    let engine = MatchingEngine::new();

    // 1. FOK with partial depth available -> REJECTED without modifying book
    let sell = OrderBuilder::new("BTCUSDT", Side::Sell, dec!(2.0))
        .price(dec!(50000))
        .build();
    engine.submit_order(sell).unwrap();

    let fok_buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(5.0))
        .price(dec!(50000))
        .time_in_force(TimeInForce::FillOrKill)
        .build();
    let events = engine.submit_order(fok_buy).unwrap();

    assert!(events.iter().any(|e| matches!(e, TradeEvent::OrderRejected { .. })));
    // Resting sell is untouched
    let snap = engine.get_snapshot("BTCUSDT", 5).unwrap();
    assert_eq!(snap.best_ask, Some(dec!(50000)));
    assert_eq!(snap.ask_depth, dec!(2.0));

    // 2. IOC with partial depth available -> Partial Fill and remainder Cancelled
    let ioc_buy = OrderBuilder::new("BTCUSDT", Side::Buy, dec!(5.0))
        .price(dec!(50000))
        .time_in_force(TimeInForce::ImmediateOrCancel)
        .build();
    let ioc_events = engine.submit_order(ioc_buy).unwrap();

    assert!(ioc_events.iter().any(|e| matches!(e, TradeEvent::TradeExecuted { .. })));
    assert!(ioc_events.iter().any(|e| matches!(e, TradeEvent::OrderCancelled { .. })));

    // Resting sell is completely consumed
    let snap_after = engine.get_snapshot("BTCUSDT", 5).unwrap();
    assert_eq!(snap_after.best_ask, None);
    assert_eq!(snap_after.best_bid, None);
}

#[tokio::test]
async fn test_grpc_end_to_end_service() {
    let engine = Arc::new(MatchingEngine::new());
    let service = OrderMatchingServiceImpl::new(Arc::clone(&engine));

    // 1. Submit Sell Limit Order via gRPC Service
    let sell_req = Request::new(PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: proto::Side::Sell as i32,
        order_type: proto::OrderType::Limit as i32,
        price: Some("50000".to_string()),
        quantity: "2.5".to_string(),
        time_in_force: proto::TimeInForce::GoodTillCancel as i32,
        stop_price: None,
        expire_at_unix_ms: None,
        custom_order_id: None,
    });

    let sell_res = service.place_order(sell_req).await.unwrap().into_inner();
    assert!(sell_res.success);
    assert_eq!(sell_res.status, proto::OrderStatus::New as i32);
    let sell_order_id = sell_res.order_id;

    // 2. Query snapshot via gRPC Service
    let book_req = Request::new(GetOrderBookRequest {
        symbol: "BTCUSDT".to_string(),
        depth: 10,
    });
    let book_res = service.get_order_book(book_req).await.unwrap().into_inner();

    assert!(book_res.success);
    assert_eq!(book_res.snapshot.unwrap().best_ask, Some("50000".to_string()));

    // 3. Cancel order via gRPC Service
    let cancel_req = Request::new(CancelOrderRequest {
        symbol: "BTCUSDT".to_string(),
        order_id: sell_order_id,
    });
    let cancel_res = service.cancel_order(cancel_req).await.unwrap().into_inner();

    assert!(cancel_res.success);
    assert_eq!(cancel_res.status, proto::OrderStatus::Cancelled as i32);
}
