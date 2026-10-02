//! # Order Matching Engine
//!
//! A high-performance, multi-symbol order matching engine with:
//!
//! - **Price-time priority** matching (FIFO within each price level)
//! - **Concurrent symbol access** via `DashMap`
//! - **Lock-free event streaming** via `crossbeam::channel`
//! - Support for Limit, Market, IOC, FOK, StopLimit, and StopMarket orders
//! - Partial fills and full order lifecycle management
//! - **Tonic gRPC server** with Protocol Buffers for order placement and streaming orderbook updates
//!
//! ## Architecture
//!
//! ```text
//!  MatchingEngine (DashMap<Symbol, OrderBook>)
//!       │
//!       ├── OrderBook (BTreeMap bids/asks + VecDeque per price level)
//!       ├── StopOrderManager (pending stop orders per symbol)
//!       ├── crossbeam::channel::Sender<TradeEvent>
//!       └── Tonic gRPC Service (OrderPlacement, OrderBook & Event Streaming)
//! ```

pub mod engine;
pub mod grpc;
pub mod mock_flow;
pub mod orderbook;
pub mod snapshot;
pub mod stop_manager;

pub use engine::MatchingEngine;
pub use grpc::{
    proto, start_grpc_server, OrderMatchingService, OrderMatchingServiceImpl,
    OrderMatchingServiceServer,
};
pub use mock_flow::{run_mock_flow, seed_market_liquidity, MarketConfig};
pub use orderbook::OrderBook;
pub use snapshot::OrderBookSnapshot;
pub use stop_manager::StopOrderManager;
