//! Binary entrypoint for the Market Data WebSocket Service.
//!
//! Connects to the Order Matching Engine gRPC server (`order-matching-engine` on port 50051),
//! ingests real trades and orderbook updates from the Rust engine, computes multi-timeframe
//! candlesticks and technical indicators, and broadcasts them over WebSockets (port 8080).
//!
//! Run with: `cargo run -p market-data-service`

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use tracing::{debug, info, warn};

use market_data_service::{
    BookLevel, CandlestickAggregator, MarketDataBroadcaster, MarketDataWsServer,
    OrderBookAnalytics, SubscriptionManager, TechnicalIndicatorsEngine, Timeframe,
};
use order_matching_engine::proto::{
    self, order_matching_service_client::OrderMatchingServiceClient, GetOrderBookRequest,
    StreamEventsRequest,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialise structured logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    info!("=== Initializing Market Data Service ===");

    let host = std::env::var("WS_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("WS_PORT")
        .or_else(|_| std::env::var("MARKET_DATA_WS_PORT"))
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .unwrap_or(8080);

    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    let grpc_url = std::env::var("ORDER_MATCHING_GRPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

    let subscriptions = Arc::new(SubscriptionManager::new());
    let broadcaster = Arc::new(MarketDataBroadcaster::new(Arc::clone(&subscriptions)));
    let server = MarketDataWsServer::new(Arc::clone(&broadcaster), Arc::clone(&subscriptions));

    // Spawn the gRPC bridge task that connects to order-matching-engine,
    // streaming real events and orderbook depths from the Rust OrderBook into the WebSocket broadcaster.
    let bcast = Arc::clone(&broadcaster);
    tokio::spawn(async move {
        run_grpc_engine_bridge(grpc_url, bcast).await;
    });

    info!("WebSocket server listening on {}", addr);

    // Run server with graceful shutdown
    tokio::select! {
        res = server.run(addr) => {
            if let Err(e) = res {
                tracing::error!("WebSocket server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received, closing Market Data WebSocket server...");
        }
    }

    info!("Market Data Service stopped gracefully");
    Ok(())
}

/// Bridges the gRPC OrderMatchingEngine to the WebSocket broadcaster.
async fn run_grpc_engine_bridge(
    grpc_url: String,
    broadcaster: Arc<MarketDataBroadcaster>,
) {
    let mut aggregators: HashMap<&'static str, CandlestickAggregator> = HashMap::new();
    for sym in ["BTC-USD", "ETH-USD", "SOL-USD"] {
        aggregators.insert(sym, CandlestickAggregator::with_standard_timeframes());
    }

    loop {
        info!(%grpc_url, "Connecting to OrderMatchingEngine gRPC service...");
        match OrderMatchingServiceClient::connect(grpc_url.clone()).await {
            Ok(mut client) => {
                info!(%grpc_url, "Connected to OrderMatchingEngine! Streaming Rust OrderBook events to WebSocket...");

                // 1. Spawn background orderbook depth poller for active symbols
                let mut ob_client = client.clone();
                let ob_bcast = Arc::clone(&broadcaster);
                let ob_handle = tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_millis(150));
                    let symbols = [
                        ("BTC-USD", "1"),
                        ("ETH-USD", "2"),
                        ("SOL-USD", "3"),
                    ];

                    loop {
                        interval.tick().await;

                        for (sym, m_id) in symbols {
                            let req = GetOrderBookRequest {
                                symbol: sym.to_string(),
                                depth: 10,
                            };

                            if let Ok(resp) = ob_client.get_order_book(req).await {
                                if let Some(snap) = resp.into_inner().snapshot {
                                    let bids: Vec<BookLevel> = snap
                                        .bids
                                        .iter()
                                        .filter_map(|l| {
                                            let px = Decimal::from_str(&l.price).ok()?;
                                            let sz = Decimal::from_str(&l.total_quantity).ok()?;
                                            Some(BookLevel::new(px, sz))
                                        })
                                        .collect();

                                    let asks: Vec<BookLevel> = snap
                                        .asks
                                        .iter()
                                        .filter_map(|l| {
                                            let px = Decimal::from_str(&l.price).ok()?;
                                            let sz = Decimal::from_str(&l.total_quantity).ok()?;
                                            Some(BookLevel::new(px, sz))
                                        })
                                        .collect();

                                    if !bids.is_empty() || !asks.is_empty() {
                                        // Broadcast raw orderbook depth
                                        ob_bcast
                                            .broadcast_orderbook_update(
                                                sym,
                                                Some(m_id.to_string()),
                                                bids.clone(),
                                                asks.clone(),
                                            )
                                            .await;

                                        // Broadcast enriched orderbook snapshot with analytics
                                        let enriched = OrderBookAnalytics::generate_enriched_snapshot(
                                            sym,
                                            Some(m_id.to_string()),
                                            bids,
                                            asks,
                                            10,
                                        );
                                        ob_bcast.broadcast_enriched_orderbook(enriched).await;
                                    }
                                }
                            }
                        }
                    }
                });

                // 2. Stream real trade events from the Rust MatchingEngine
                let stream_req = StreamEventsRequest { symbol: None };
                match client.stream_events(stream_req).await {
                    Ok(resp) => {
                        let mut event_stream = resp.into_inner();
                        while let Ok(Some(event_msg)) = event_stream.message().await {
                            if let Some(proto::trade_event_msg::Event::TradeExecuted(
                                proto::TradeExecutedEvent { trade: Some(t) },
                            )) = event_msg.event
                            {
                                        let price = Decimal::from_str(&t.price).unwrap_or_default();
                                        let qty = Decimal::from_str(&t.quantity).unwrap_or_default();
                                        let is_maker_buy = t.maker_side == proto::Side::Buy as i32;
                                        let taker_side = if is_maker_buy { "sell" } else { "buy" };
                                        let trade_time = Utc
                                            .timestamp_millis_opt(t.created_at_unix_ms)
                                            .single()
                                            .unwrap_or_else(Utc::now);

                                        let m_id = match t.symbol.as_str() {
                                            "BTC-USD" => "1",
                                            "ETH-USD" => "2",
                                            "SOL-USD" => "3",
                                            _ => "1",
                                        };

                                        // 1. Broadcast price update
                                        broadcaster
                                            .broadcast_price_update(&t.symbol, Some(m_id.to_string()), price)
                                            .await;

                                        // 2. Broadcast trade execution
                                        broadcaster
                                            .broadcast_trade(
                                                &t.id,
                                                &t.symbol,
                                                Some(m_id.to_string()),
                                                price,
                                                qty,
                                                taker_side,
                                            )
                                            .await;

                                        // 3. Ingest trade into CandlestickAggregator
                                        let sym_key = match t.symbol.as_str() {
                                            "BTC-USD" => "BTC-USD",
                                            "ETH-USD" => "ETH-USD",
                                            "SOL-USD" => "SOL-USD",
                                            _ => "BTC-USD",
                                        };
                                        if let Some(agg) = aggregators.get_mut(sym_key) {
                                            let candle_events = agg.ingest_trade(sym_key, price, qty, trade_time);
                                            for ev in candle_events {
                                                match ev {
                                                    market_data_service::CandleEvent::Updated(c) => {
                                                        broadcaster
                                                            .broadcast_candle_update(
                                                                sym_key,
                                                                Some(m_id.to_string()),
                                                                c.timeframe,
                                                                c,
                                                            )
                                                            .await;
                                                    }
                                                    market_data_service::CandleEvent::Closed {
                                                        closed_candle,
                                                        new_candle,
                                                    } => {
                                                        broadcaster
                                                            .broadcast_candle_update(
                                                                sym_key,
                                                                Some(m_id.to_string()),
                                                                closed_candle.timeframe,
                                                                closed_candle,
                                                            )
                                                            .await;
                                                        broadcaster
                                                            .broadcast_candle_update(
                                                                sym_key,
                                                                Some(m_id.to_string()),
                                                                new_candle.timeframe,
                                                                new_candle,
                                                            )
                                                            .await;
                                                    }
                                                }
                                            }

                                            // 4. Compute & broadcast technical indicators snapshot
                                            for tf in [Timeframe::OneSecond, Timeframe::OneMinute] {
                                                let history = agg.get_all_candles(sym_key, tf);
                                                if let Some(ind) =
                                                    TechnicalIndicatorsEngine::compute_snapshot(
                                                        sym_key, tf, &history,
                                                    )
                                                {
                                                    broadcaster
                                                        .broadcast_indicators_update(
                                                            sym_key,
                                                            Some(m_id.to_string()),
                                                            tf,
                                                            ind,
                                                        )
                                                        .await;
                                                }
                                            }
                                        }
                                    }
                                }
                    }
                    Err(e) => {
                        warn!("Failed to subscribe to gRPC event stream: {}", e);
                    }
                }

                ob_handle.abort();
            }
            Err(e) => {
                debug!("Waiting for OrderMatchingEngine gRPC at {}: {}", grpc_url, e);
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
