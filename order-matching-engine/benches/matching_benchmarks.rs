//! Criterion microbenchmarks for the Order Matching Engine,
//! specifically measuring throughput for 100,000+ orders per second.

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use order_matching_engine::{MatchingEngine, OrderBook};
use rust_decimal::Decimal;
use shared::{MatchingEngineOps, OrderBookOps, OrderBuilder, OrderType, Side};

/// Benchmark 1: Inserting 100,000 resting limit orders into a single order book.
fn bench_100k_resting_order_insertions(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_throughput_orderbook");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(100_000));

    // Pre-generate 100k order templates to measure pure orderbook insertion speed
    let orders: Vec<_> = (1..=100_000)
        .map(|i| {
            let price = Decimal::new(50000 + (i % 2000), 0);
            OrderBuilder::new("BTCUSDT", Side::Sell, Decimal::ONE)
                .price(price)
                .build()
        })
        .collect();

    group.bench_function("insert_100k_limit_orders", |b| {
        b.iter(|| {
            let mut book = OrderBook::new("BTCUSDT");
            for order in &orders {
                let mut o = order.clone();
                book.process_order(black_box(&mut o));
            }
            assert_eq!(book.order_count(), 100_000);
        });
    });

    group.finish();
}

/// Benchmark 2: Continuous 100,000 crossed order match executions (50k resting makers + 50k aggressive takers).
fn bench_100k_continuous_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_throughput_matching");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(100_000));

    let makers: Vec<_> = (1..=50_000)
        .map(|i| {
            let price = Decimal::new(50000 + (i % 100), 0);
            OrderBuilder::new("BTCUSDT", Side::Sell, Decimal::ONE)
                .price(price)
                .build()
        })
        .collect();

    let takers: Vec<_> = (1..=50_000)
        .map(|i| {
            let price = Decimal::new(50000 + (i % 100), 0);
            OrderBuilder::new("BTCUSDT", Side::Buy, Decimal::ONE)
                .price(price)
                .build()
        })
        .collect();

    group.bench_function("match_100k_orders_continuous", |b| {
        b.iter(|| {
            let mut book = OrderBook::new("BTCUSDT");

            // Interleave 50,000 maker-taker pairs (100k total operations)
            for i in 0..50_000 {
                let mut m = makers[i].clone();
                book.process_order(&mut m);

                let mut t = takers[i].clone();
                book.process_order(black_box(&mut t));
            }

            assert_eq!(book.order_count(), 0);
        });
    });

    group.finish();
}

/// Benchmark 3: Market order sweeping through deep 1,000 price levels.
fn bench_deep_market_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("market_sweep");
    group.throughput(Throughput::Elements(1_000));

    let levels: Vec<_> = (1..=1_000)
        .map(|i| {
            let price = Decimal::new(50000 + i, 0);
            OrderBuilder::new("BTCUSDT", Side::Sell, Decimal::ONE)
                .price(price)
                .build()
        })
        .collect();

    group.bench_function("sweep_1000_levels", |b| {
        b.iter(|| {
            let mut book = OrderBook::new("BTCUSDT");
            for order in &levels {
                let mut o = order.clone();
                book.process_order(&mut o);
            }

            let mut market_buy = OrderBuilder::new("BTCUSDT", Side::Buy, Decimal::new(1000, 0))
                .order_type(OrderType::Market)
                .build();
            book.process_order(black_box(&mut market_buy));
            assert_eq!(book.order_count(), 0);
        });
    });

    group.finish();
}

/// Benchmark 4: Multi-threaded concurrent order submission across 8 symbols (100k orders total).
fn bench_multi_threaded_100k_orders(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_symbol_100k");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(100_000));

    let symbols = [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "DOTUSDT", "AVAXUSDT", "LINKUSDT", "MATICUSDT",
    ];

    let orders_per_symbol = 12_500; // 8 * 12,500 = 100,000 orders
    let all_orders: Vec<_> = symbols
        .iter()
        .flat_map(|sym| {
            (1..=orders_per_symbol).map(move |i| {
                let price = Decimal::new(1000 + (i % 500), 0);
                OrderBuilder::new(*sym, Side::Sell, Decimal::ONE)
                    .price(price)
                    .build()
            })
        })
        .collect();

    group.bench_function("submit_100k_orders_engine", |b| {
        b.iter(|| {
            let engine = MatchingEngine::new();
            for order in &all_orders {
                let _ = engine.submit_order(black_box(order.clone()));
            }
            assert_eq!(engine.total_order_count(), 100_000);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_100k_resting_order_insertions,
    bench_100k_continuous_matching,
    bench_deep_market_sweep,
    bench_multi_threaded_100k_orders
);
criterion_main!(benches);
