//! Main entrypoint starting the Order Matching Engine gRPC server.
//!
//! Run with: `cargo run -p order-matching-engine`

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;
use tracing::info;

use order_matching_engine::{
    MatchingEngine, OrderMatchingServiceImpl, OrderMatchingServiceServer,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialise structured logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    info!("=== Initializing Order Matching Engine ===");

    let engine = Arc::new(MatchingEngine::new());

    // Start background mock liquidity & trade flow task feeding directly into the Rust OrderBook
    let enable_mock = std::env::var("ENABLE_MOCK_FLOW").unwrap_or_else(|_| "true".to_string()) != "false";
    if enable_mock {
        let mock_engine = Arc::clone(&engine);
        tokio::spawn(async move {
            order_matching_engine::run_mock_flow(mock_engine).await;
        });
    }

    // Configure gRPC address
    let port = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "50051".to_string())
        .parse::<u16>()
        .unwrap_or(50051);

    let host = std::env::var("GRPC_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    let service = OrderMatchingServiceImpl::new(Arc::clone(&engine));
    let server = OrderMatchingServiceServer::new(service);

    info!("gRPC server listening on {}", addr);
    info!("Endpoints available:");
    info!("  - PlaceOrder        (Unary RPC)");
    info!("  - CancelOrder       (Unary RPC)");
    info!("  - GetOrderBook      (Unary RPC)");
    info!("  - StreamOrderBook   (Server Streaming RPC)");
    info!("  - StreamEvents      (Server Streaming RPC)");

    Server::builder()
        .add_service(server)
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl+c signal");
            info!("Shutdown signal received, shutting down gRPC server...");
        })
        .await?;

    info!("Order Matching Engine terminated gracefully");
    Ok(())
}
