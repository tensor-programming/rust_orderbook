//! Criterion microbenchmarks for Market Data WebSocket Broadcaster & Aggregation Engine.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use market_data_service::{
    BookLevel, Candlestick, CandlestickAggregator, Channel, MarketDataBroadcaster,
    OrderBookAnalytics, SubscriptionManager, TechnicalIndicatorsEngine, Timeframe, Topic,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use uuid::Uuid;

/// Benchmark 1: Topic lookup and subscriber retrieval across 10,000 active client subscriptions.
fn bench_10k_subscription_lookup(c: &mut Criterion) {
    let mgr = SubscriptionManager::new();

    for i in 0..10_000 {
        let id = Uuid::new_v4();
        let sym = match i % 3 {
            0 => "BTC-USD",
            1 => "ETH-USD",
            _ => "SOL-USD",
        };
        mgr.subscribe(&id, Topic::new(sym, Channel::OrderBook));
    }

    let mut group = c.benchmark_group("high_concurrency_subscriptions");
    group.sample_size(10);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("lookup_subscribers_in_10k_pool", |b| {
        b.iter(|| {
            let res = mgr.get_subscribers_for(black_box("BTC-USD"), black_box(Channel::OrderBook));
            assert!(res.len() >= 3333);
        });
    });

    group.finish();
}

/// Benchmark 2: Real-time OrderBook depth broadcast fan-out to 10,000 concurrent client queues.
fn bench_10k_concurrent_orderbook_broadcast(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let subs = Arc::new(SubscriptionManager::new());
    let broadcaster = Arc::new(MarketDataBroadcaster::new(Arc::clone(&subs)));

    // Register 10,000 concurrent client queues
    let mut receivers = Vec::with_capacity(10_000);
    for _ in 0..10_000 {
        let id = Uuid::new_v4();
        let (tx, rx) = mpsc::channel(128);
        broadcaster.register_client(id, tx);
        subs.subscribe(&id, Topic::new("BTC-USD", Channel::OrderBook));
        receivers.push(rx);
    }

    assert_eq!(subs.active_client_count(), 10_000);

    let mut group = c.benchmark_group("high_concurrency_broadcast");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(10_000));

    let bcast = Arc::clone(&broadcaster);
    group.bench_function("broadcast_orderbook_to_10k_clients", |b| {
        b.to_async(&rt).iter(|| async {
            let bids = vec![
                BookLevel::new(dec!(52300), dec!(1.5)),
                BookLevel::new(dec!(52290), dec!(3.2)),
            ];
            let asks = vec![
                BookLevel::new(dec!(52310), dec!(1.8)),
                BookLevel::new(dec!(52320), dec!(4.0)),
            ];
            bcast
                .broadcast_orderbook_update(
                    black_box("BTC-USD"),
                    black_box(Some("1".to_string())),
                    black_box(bids),
                    black_box(asks),
                )
                .await;
        });
    });

    group.finish();
}

/// Benchmark 3: Candlestick Aggregation Throughput (100,000 trade ingestions).
fn bench_candlestick_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("candlestick_aggregation");
    group.sample_size(10);
    group.throughput(Throughput::Elements(100_000));

    let now = Utc::now();
    let trades: Vec<_> = (1..=100_000)
        .map(|i| {
            let price = Decimal::new(50000 + (i % 200), 0);
            let qty = Decimal::new(1 + (i % 5), 0);
            (price, qty)
        })
        .collect();

    group.bench_function("ingest_100k_trades_multi_timeframe", |b| {
        b.iter(|| {
            let mut aggregator = CandlestickAggregator::with_standard_timeframes();
            for (price, qty) in &trades {
                aggregator.ingest_trade("BTCUSDT", *price, *qty, now);
            }
        });
    });

    group.finish();
}

/// Benchmark 4: Technical Indicators computation on 1,000 bars.
fn bench_technical_indicators_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("technical_indicators");
    group.throughput(Throughput::Elements(1_000));

    let candles: Vec<_> = (1..=1_000)
        .map(|i| {
            let price = Decimal::new(50000 + (i % 500), 0);
            Candlestick::new("BTCUSDT", Timeframe::OneMinute, (i as i64) * 60_000, price, dec!(2.5))
        })
        .collect();

    group.bench_function("compute_full_indicators_snapshot_1000_bars", |b| {
        b.iter(|| {
            let snapshot = TechnicalIndicatorsEngine::compute_snapshot(
                black_box("BTCUSDT"),
                black_box(Timeframe::OneMinute),
                black_box(&candles),
            );
            assert!(snapshot.is_some());
        });
    });

    group.finish();
}

/// Benchmark 5: Enriched OrderBook analytics calculation (micro-price, imbalance, spread).
fn bench_orderbook_analytics(c: &mut Criterion) {
    let mut group = c.benchmark_group("orderbook_analytics");
    group.throughput(Throughput::Elements(10_000));

    let bids: Vec<_> = (1..=50)
        .map(|i| BookLevel::new(Decimal::new(50000 - i * 5, 0), Decimal::new(i, 1)))
        .collect();
    let asks: Vec<_> = (1..=50)
        .map(|i| BookLevel::new(Decimal::new(50000 + i * 5, 0), Decimal::new(i, 1)))
        .collect();

    group.bench_function("generate_10k_enriched_snapshots", |b| {
        b.iter(|| {
            for _ in 0..10_000 {
                let snap = OrderBookAnalytics::generate_enriched_snapshot(
                    black_box("BTCUSDT"),
                    black_box(Some("1".to_string())),
                    black_box(bids.clone()),
                    black_box(asks.clone()),
                    black_box(20),
                );
                assert!(snap.micro_price.is_some());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_10k_subscription_lookup,
    bench_10k_concurrent_orderbook_broadcast,
    bench_candlestick_aggregation,
    bench_technical_indicators_engine,
    bench_orderbook_analytics
);
criterion_main!(benches);
