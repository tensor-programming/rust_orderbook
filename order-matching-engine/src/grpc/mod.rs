//! gRPC server and protocol buffer integration for the Order Matching Engine.

pub mod proto_convert;
pub mod service;

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;
use tracing::info;

pub use proto_convert::proto;
pub use proto_convert::proto::order_matching_service_server::{
    OrderMatchingService, OrderMatchingServiceServer,
};
pub use service::OrderMatchingServiceImpl;

use crate::engine::MatchingEngine;

/// Start the Tonic gRPC server on the given address.
pub async fn start_grpc_server(
    engine: Arc<MatchingEngine>,
    addr: SocketAddr,
) -> Result<(), tonic::transport::Error> {
    let service = OrderMatchingServiceImpl::new(engine);
    let server = OrderMatchingServiceServer::new(service);

    info!(%addr, "Starting OrderMatchingEngine gRPC server");

    Server::builder()
        .add_service(server)
        .serve(addr)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::proto_convert::proto::{
        self, CancelOrderRequest, GetOrderBookRequest, PlaceOrderRequest,
    };
    use tonic::Request;

    #[tokio::test]
    async fn test_grpc_place_and_match_order() {
        let engine = Arc::new(MatchingEngine::new());
        let service = OrderMatchingServiceImpl::new(Arc::clone(&engine));

        // 1. Place a resting Sell Limit order
        let sell_req = Request::new(PlaceOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: proto::Side::Sell as i32,
            order_type: proto::OrderType::Limit as i32,
            price: Some("50000".to_string()),
            quantity: "1.0".to_string(),
            time_in_force: proto::TimeInForce::GoodTillCancel as i32,
            stop_price: None,
            expire_at_unix_ms: None,
            custom_order_id: None,
        });

        let sell_res = service.place_order(sell_req).await.unwrap().into_inner();
        assert!(sell_res.success);
        assert_eq!(sell_res.status, proto::OrderStatus::New as i32);

        // 2. Query OrderBook snapshot
        let book_req = Request::new(GetOrderBookRequest {
            symbol: "BTCUSDT".to_string(),
            depth: 10,
        });
        let book_res = service.get_order_book(book_req).await.unwrap().into_inner();

        assert!(book_res.success);
        let snapshot = book_res.snapshot.unwrap();
        assert_eq!(snapshot.best_ask, Some("50000".to_string()));

        // 3. Place crossing Buy Limit order -> Full Fill
        let buy_req = Request::new(PlaceOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: proto::Side::Buy as i32,
            order_type: proto::OrderType::Limit as i32,
            price: Some("50000".to_string()),
            quantity: "1.0".to_string(),
            time_in_force: proto::TimeInForce::GoodTillCancel as i32,
            stop_price: None,
            expire_at_unix_ms: None,
            custom_order_id: None,
        });

        let buy_res = service.place_order(buy_req).await.unwrap().into_inner();
        assert!(buy_res.success);
        assert_eq!(buy_res.status, proto::OrderStatus::Filled as i32);
        assert!(!buy_res.events.is_empty());
    }

    #[tokio::test]
    async fn test_grpc_cancel_order() {
        let engine = Arc::new(MatchingEngine::new());
        let service = OrderMatchingServiceImpl::new(Arc::clone(&engine));

        // Place order
        let place_req = Request::new(PlaceOrderRequest {
            symbol: "ETHUSDT".to_string(),
            side: proto::Side::Buy as i32,
            order_type: proto::OrderType::Limit as i32,
            price: Some("3000".to_string()),
            quantity: "2.0".to_string(),
            time_in_force: proto::TimeInForce::GoodTillCancel as i32,
            stop_price: None,
            expire_at_unix_ms: None,
            custom_order_id: None,
        });

        let place_res = service.place_order(place_req).await.unwrap().into_inner();
        let order_id = place_res.order_id;

        // Cancel order
        let cancel_req = Request::new(CancelOrderRequest {
            symbol: "ETHUSDT".to_string(),
            order_id: order_id.clone(),
        });
        let cancel_res = service.cancel_order(cancel_req).await.unwrap().into_inner();

        assert!(cancel_res.success);
        assert_eq!(cancel_res.status, proto::OrderStatus::Cancelled as i32);
    }
}
