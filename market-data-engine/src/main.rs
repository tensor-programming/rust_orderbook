//! Binary entrypoint for the Market Data WebSocket Service.
//!
//! Run with: `cargo run -p market-data-service`

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rust_decimal_macros::dec;
use tracing::info;

use market_data_service::{
    BookLevel, MarketDataBroadcaster, MarketDataWsServer, SubscriptionManager,
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

    let subscriptions = Arc::new(SubscriptionManager::new());
    let broadcaster = Arc::new(MarketDataBroadcaster::new(Arc::clone(&subscriptions)));
    let server = MarketDataWsServer::new(Arc::clone(&broadcaster), Arc::clone(&subscriptions));

    // Optional background task broadcasting periodic market updates (BTC-USD, ETH-USD)
    let bcast = Arc::clone(&broadcaster);
    tokio::spawn(async move {
        let mut btc_price = dec!(52345.67);
        let mut eth_price = dec!(3123.45);
        let mut interval = tokio::time::interval(Duration::from_millis(1000));

        loop {
            interval.tick().await;

            // Small jitter for simulation
            btc_price += dec!(0.50);
            eth_price += dec!(0.15);

            // Broadcast price updates
            bcast
                .broadcast_price_update("BTC-USD", Some("1".to_string()), btc_price)
                .await;
            bcast
                .broadcast_price_update("ETH-USD", Some("2".to_string()), eth_price)
                .await;

            // Broadcast orderbook updates
            let bids = vec![
                BookLevel::new(btc_price - dec!(10.0), dec!(1.5)),
                BookLevel::new(btc_price - dec!(20.0), dec!(3.0)),
                BookLevel::new(btc_price - dec!(30.0), dec!(5.2)),
            ];
            let asks = vec![
                BookLevel::new(btc_price + dec!(10.0), dec!(1.2)),
                BookLevel::new(btc_price + dec!(20.0), dec!(2.8)),
                BookLevel::new(btc_price + dec!(30.0), dec!(4.5)),
            ];

            bcast
                .broadcast_orderbook_update("BTC-USD", Some("1".to_string()), bids, asks)
                .await;
        }
    });

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
