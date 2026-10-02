//! Integration and unit tests for Market Data WebSocket Server, Aggregation & PubSub Backpressure.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use market_data_service::{
    calculate_bollinger_bands, calculate_ema, calculate_rsi, calculate_sma, calculate_vwap,
    calculate_wma, BackpressureQueue, BackpressureStrategy, BookLevel, CandleEvent, Candlestick,
    CandlestickAggregator, ClientMessage, MarketDataBroadcaster, MarketDataPubSub,
    MarketDataWsServer, OrderBookAnalytics, ServerMessage, SubscriptionManager,
    TechnicalIndicatorsEngine, Timeframe, Topic,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

// ═══════════════════════════════════════════════════════════════════
// PubSub & Backpressure Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_backpressure_drop_oldest() {
    // Queue capacity = 3, strategy = DropOldest
    let queue = BackpressureQueue::new(3, BackpressureStrategy::DropOldest);

    queue.push(1);
    queue.push(2);
    queue.push(3);
    assert_eq!(queue.len(), 3);

    // Push 4th and 5th items -> should evict 1 and 2
    queue.push(4);
    queue.push(5);
    assert_eq!(queue.len(), 3);

    // Popped items should be the freshest: 3, 4, 5
    assert_eq!(queue.try_pop(), Some(3));
    assert_eq!(queue.try_pop(), Some(4));
    assert_eq!(queue.try_pop(), Some(5));
    assert_eq!(queue.try_pop(), None);

    assert_eq!(
        queue
            .metrics()
            .total_pushed
            .load(std::sync::atomic::Ordering::Relaxed),
        5
    );
    assert_eq!(
        queue
            .metrics()
            .total_dropped
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(
        queue
            .metrics()
            .total_consumed
            .load(std::sync::atomic::Ordering::Relaxed),
        3
    );
}

#[test]
fn test_backpressure_drop_latest() {
    // Queue capacity = 3, strategy = DropLatest
    let queue = BackpressureQueue::new(3, BackpressureStrategy::DropLatest);

    assert!(queue.push(10));
    assert!(queue.push(20));
    assert!(queue.push(30));
    assert_eq!(queue.len(), 3);

    // 4th and 5th items should be rejected/dropped
    assert!(!queue.push(40));
    assert!(!queue.push(50));
    assert_eq!(queue.len(), 3);

    // Popped items should be the original: 10, 20, 30
    assert_eq!(queue.try_pop(), Some(10));
    assert_eq!(queue.try_pop(), Some(20));
    assert_eq!(queue.try_pop(), Some(30));
    assert_eq!(queue.try_pop(), None);
}

#[test]
fn test_backpressure_throttle() {
    // Throttle with 50ms interval
    let queue = BackpressureQueue::new(
        5,
        BackpressureStrategy::Throttle {
            min_interval_ms: 50,
        },
    );

    queue.push(100);
    // Rapid pushes immediately after should coalesce/replace
    queue.push(101);
    queue.push(102);

    // Buffer should hold initial + throttled replacement
    assert!(queue.len() <= 2);
    let first = queue.try_pop();
    assert!(first == Some(100) || first == Some(102));
}

#[test]
fn test_market_data_pubsub_dispatch() {
    let subscriptions = Arc::new(SubscriptionManager::new());
    let pubsub = MarketDataPubSub::new(Arc::clone(&subscriptions));

    let client_a = Uuid::new_v4();
    let client_b = Uuid::new_v4();

    // Client A subscribes to BTC-USD OrderBook with DropOldest (capacity 2)
    let queue_a = pubsub.register_subscriber(client_a, 2, BackpressureStrategy::DropOldest);
    pubsub.subscribe(
        &client_a,
        Topic::new("BTC-USD", market_data_service::Channel::OrderBook),
    );

    // Client B subscribes to ETH-USD Trades
    let queue_b = pubsub.register_subscriber(client_b, 5, BackpressureStrategy::DropLatest);
    pubsub.subscribe(
        &client_b,
        Topic::new("ETH-USD", market_data_service::Channel::Trades),
    );

    // Publish BTC-USD OrderBook updates
    let msg1 = ServerMessage::OrderBookUpdate {
        market_id: Some("1".to_string()),
        symbol: "BTC-USD".to_string(),
        bids: vec![],
        asks: vec![],
        timestamp: 1000,
    };
    let msg2 = ServerMessage::OrderBookUpdate {
        market_id: Some("1".to_string()),
        symbol: "BTC-USD".to_string(),
        bids: vec![],
        asks: vec![],
        timestamp: 2000,
    };
    let msg3 = ServerMessage::OrderBookUpdate {
        market_id: Some("1".to_string()),
        symbol: "BTC-USD".to_string(),
        bids: vec![],
        asks: vec![],
        timestamp: 3000,
    };

    pubsub.publish("BTC-USD", market_data_service::Channel::OrderBook, msg1);
    pubsub.publish("BTC-USD", market_data_service::Channel::OrderBook, msg2);
    pubsub.publish("BTC-USD", market_data_service::Channel::OrderBook, msg3);

    // Client A queue had capacity 2 -> msg1 dropped, msg2 & msg3 present
    assert_eq!(queue_a.len(), 2);
    let popped = queue_a.try_pop().unwrap();
    match popped {
        ServerMessage::OrderBookUpdate { timestamp, .. } => assert_eq!(timestamp, 2000),
        _ => panic!("Expected OrderBookUpdate"),
    }

    // Client B should have received 0 messages (different symbol/channel)
    assert_eq!(queue_b.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════
// WebSocket Server & Subscription Integration Tests
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_fake_ws_client_connect_and_welcome() {
    let subscriptions = Arc::new(SubscriptionManager::new());
    let broadcaster = Arc::new(MarketDataBroadcaster::new(Arc::clone(&subscriptions)));
    let server = MarketDataWsServer::new(Arc::clone(&broadcaster), Arc::clone(&subscriptions));

    let mut client = server.create_mock_client();

    let first_msg = client.recv().await.expect("Expected welcome message");
    match first_msg {
        ServerMessage::Connected { message } => {
            assert!(message.contains("Connected to Market Data"));
        }
        _ => panic!("Expected Connected server message"),
    }
}

#[tokio::test]
async fn test_fake_ws_subscription_and_orderbook_broadcast() {
    let subscriptions = Arc::new(SubscriptionManager::new());
    let broadcaster = Arc::new(MarketDataBroadcaster::new(Arc::clone(&subscriptions)));
    let server = MarketDataWsServer::new(Arc::clone(&broadcaster), Arc::clone(&subscriptions));

    let mut client = server.create_mock_client();
    let _ = client.recv().await.unwrap(); // Welcome

    // Send subscribe request
    client
        .send_message(ClientMessage::Subscribe {
            market_id: Some("1".to_string()),
            symbol: Some("BTC-USD".to_string()),
            channel: Some("orderbook".to_string()),
            timeframe: None,
            backpressure: Some("drop_oldest".to_string()),
            throttle_ms: None,
        })
        .await;

    let sub_ack = client.recv().await.expect("Expected sub ack");
    match sub_ack {
        ServerMessage::Subscribed {
            channel, symbol, ..
        } => {
            assert_eq!(channel, "orderbook");
            assert_eq!(symbol, Some("BTC-USD".to_string()));
        }
        _ => panic!("Expected Subscribed message"),
    }

    // Trigger broadcast from server
    let bids = vec![BookLevel::new(dec!(50000), dec!(1.5))];
    let asks = vec![BookLevel::new(dec!(50010), dec!(2.0))];
    broadcaster
        .broadcast_orderbook_update("BTC-USD", Some("1".to_string()), bids.clone(), asks.clone())
        .await;

    let update_msg = client.recv().await.expect("Expected orderbook update");
    match update_msg {
        ServerMessage::OrderBookUpdate {
            symbol,
            market_id,
            bids: recv_bids,
            asks: recv_asks,
            ..
        } => {
            assert_eq!(symbol, "BTC-USD");
            assert_eq!(market_id, Some("1".to_string()));
            assert_eq!(recv_bids, bids);
            assert_eq!(recv_asks, asks);
        }
        _ => panic!("Expected OrderBookUpdate message"),
    }
}

#[tokio::test]
async fn test_fake_ws_candle_and_indicator_subscription() {
    let subscriptions = Arc::new(SubscriptionManager::new());
    let broadcaster = Arc::new(MarketDataBroadcaster::new(Arc::clone(&subscriptions)));
    let server = MarketDataWsServer::new(Arc::clone(&broadcaster), Arc::clone(&subscriptions));

    let mut client = server.create_mock_client();
    let _ = client.recv().await.unwrap(); // Welcome

    // Subscribe to candles for 1m timeframe
    client
        .send_message(ClientMessage::Subscribe {
            market_id: Some("1".to_string()),
            symbol: Some("BTC-USD".to_string()),
            channel: Some("candles".to_string()),
            timeframe: Some("1m".to_string()),
            backpressure: None,
            throttle_ms: None,
        })
        .await;

    let _ack = client.recv().await.unwrap();

    // Broadcast candle update
    let candle = Candlestick::new(
        "BTC-USD",
        Timeframe::OneMinute,
        1700000000000,
        dec!(52000),
        dec!(1.0),
    );
    broadcaster
        .broadcast_candle_update(
            "BTC-USD",
            Some("1".to_string()),
            Timeframe::OneMinute,
            candle.clone(),
        )
        .await;

    let msg = client.recv().await.expect("Expected candle message");
    match msg {
        ServerMessage::CandleUpdate {
            symbol,
            timeframe,
            candle: recv_candle,
            ..
        } => {
            assert_eq!(symbol, "BTC-USD");
            assert_eq!(timeframe, Timeframe::OneMinute);
            assert_eq!(recv_candle.open, dec!(52000));
        }
        _ => panic!("Expected CandleUpdate message"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Candlestick Aggregation Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_candlestick_aggregation_and_rollover() {
    let mut aggregator = CandlestickAggregator::new(vec![Timeframe::OneMinute]);

    let t0 = Utc.with_ymd_and_hms(2026, 9, 23, 12, 0, 10).unwrap();
    let t1 = Utc.with_ymd_and_hms(2026, 9, 23, 12, 0, 25).unwrap();
    let t2 = Utc.with_ymd_and_hms(2026, 9, 23, 12, 0, 45).unwrap();
    let t3_next_minute = Utc.with_ymd_and_hms(2026, 9, 23, 12, 1, 05).unwrap();

    // Ingest 3 trades in the same minute
    aggregator.ingest_trade("BTCUSDT", dec!(50000), dec!(1.0), t0);
    aggregator.ingest_trade("BTCUSDT", dec!(50500), dec!(2.0), t1);
    aggregator.ingest_trade("BTCUSDT", dec!(49800), dec!(1.5), t2);

    let active = aggregator
        .get_active_candle("BTCUSDT", Timeframe::OneMinute)
        .unwrap();
    assert_eq!(active.open, dec!(50000));
    assert_eq!(active.high, dec!(50500));
    assert_eq!(active.low, dec!(49800));
    assert_eq!(active.close, dec!(49800));
    assert_eq!(active.volume, dec!(4.5));
    assert_eq!(active.trade_count, 3);
    assert!(!active.is_closed);

    // Trade in next minute triggers rollover
    let events = aggregator.ingest_trade("BTCUSDT", dec!(50200), dec!(0.8), t3_next_minute);
    assert_eq!(events.len(), 1);

    match &events[0] {
        CandleEvent::Closed {
            closed_candle,
            new_candle,
        } => {
            assert!(closed_candle.is_closed);
            assert_eq!(closed_candle.close, dec!(49800));
            assert_eq!(new_candle.open, dec!(50200));
            assert_eq!(new_candle.trade_count, 1);
        }
        _ => panic!("Expected Closed event on minute boundary rollover"),
    }

    // Historical buffer check
    let history = aggregator.get_history("BTCUSDT", Timeframe::OneMinute);
    assert_eq!(history.len(), 1);
    assert!(history[0].is_closed);
}

// ═══════════════════════════════════════════════════════════════════
// Technical Indicators Calculation Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_technical_indicators_calculations() {
    let prices = vec![
        dec!(10),
        dec!(11),
        dec!(12),
        dec!(13),
        dec!(14),
        dec!(15),
        dec!(16),
        dec!(17),
        dec!(18),
        dec!(19),
        dec!(20),
        dec!(21),
        dec!(22),
        dec!(23),
        dec!(24),
        dec!(25),
        dec!(26),
        dec!(27),
        dec!(28),
        dec!(29),
        dec!(30),
    ];

    let sma_5 = calculate_sma(&prices, 5).unwrap();
    assert_eq!(sma_5, dec!(28));

    let ema_5 = calculate_ema(&prices, 5).unwrap();
    assert!(ema_5 > dec!(27) && ema_5 <= dec!(30));

    let wma_3 = calculate_wma(&prices, 3).unwrap();
    assert_eq!(wma_3.round_dp(2), dec!(29.33));

    let rsi = calculate_rsi(&prices, 14);
    assert!(rsi.is_some());
    let rsi_val = rsi.unwrap();
    assert_eq!(rsi_val, dec!(100));

    let bb = calculate_bollinger_bands(&prices, 20, Decimal::TWO).unwrap();
    assert!(bb.upper > bb.middle);
    assert!(bb.middle > bb.lower);
    assert_eq!(bb.middle, calculate_sma(&prices, 20).unwrap());
}

#[test]
fn test_atr_and_vwap_calculations() {
    let candles = vec![
        Candlestick::new("BTCUSDT", Timeframe::OneMinute, 1000, dec!(100), dec!(10)),
        Candlestick::new("BTCUSDT", Timeframe::OneMinute, 2000, dec!(105), dec!(20)),
        Candlestick::new("BTCUSDT", Timeframe::OneMinute, 3000, dec!(102), dec!(15)),
    ];

    let vwap = calculate_vwap(&candles).unwrap();
    assert!(vwap >= dec!(100) && vwap <= dec!(105));

    let snapshot =
        TechnicalIndicatorsEngine::compute_snapshot("BTCUSDT", Timeframe::OneMinute, &candles);
    assert!(snapshot.is_some());
    let snap = snapshot.unwrap();
    assert_eq!(snap.current_price, dec!(102));
}

// ═══════════════════════════════════════════════════════════════════
// Enriched OrderBook Analytics Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_orderbook_analytics_metrics() {
    let bids = vec![
        BookLevel::new(dec!(50000), dec!(3.0)), // L1
        BookLevel::new(dec!(49990), dec!(7.0)),
    ];
    let asks = vec![
        BookLevel::new(dec!(50020), dec!(1.0)), // L1
        BookLevel::new(dec!(50030), dec!(4.0)),
    ];

    let snap = OrderBookAnalytics::generate_enriched_snapshot(
        "BTCUSDT",
        Some("1".to_string()),
        bids,
        asks,
        10,
    );

    assert_eq!(snap.best_bid, Some(dec!(50000)));
    assert_eq!(snap.best_ask, Some(dec!(50020)));
    assert_eq!(snap.spread, Some(dec!(20)));
    assert_eq!(snap.mid_price, Some(dec!(50010)));
    assert_eq!(snap.bid_depth, dec!(10.0));
    assert_eq!(snap.ask_depth, dec!(5.0));
    assert_eq!(snap.order_imbalance.unwrap().round_dp(2), dec!(0.33));
    assert_eq!(snap.micro_price, Some(dec!(50015)));
}
