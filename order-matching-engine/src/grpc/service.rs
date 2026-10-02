//! Tonic gRPC service implementation for OrderMatchingService.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::debug;
use uuid::Uuid;

use shared::{EngineError, MatchingEngineOps, TradeEvent};

use crate::engine::MatchingEngine;
use crate::grpc::proto_convert::proto::{
    self, order_matching_service_server::OrderMatchingService, CancelOrderRequest,
    CancelOrderResponse, GetOrderBookRequest, GetOrderBookResponse, OrderBookSnapshotMsg,
    OrderBookUpdateMsg, PlaceOrderRequest, PlaceOrderResponse, StreamEventsRequest,
    StreamOrderBookRequest, TradeEventMsg,
};

pub type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// gRPC service implementation wrapping `Arc<MatchingEngine>`.
#[derive(Clone)]
pub struct OrderMatchingServiceImpl {
    engine: Arc<MatchingEngine>,
}

impl OrderMatchingServiceImpl {
    pub fn new(engine: Arc<MatchingEngine>) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl OrderMatchingService for OrderMatchingServiceImpl {
    type StreamOrderBookStream = ResponseStream<OrderBookUpdateMsg>;
    type StreamEventsStream = ResponseStream<TradeEventMsg>;

    // ───────────────────────────────────────────────────────────────
    // Place Order
    // ───────────────────────────────────────────────────────────────
    async fn place_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let req = request.into_inner();
        debug!(symbol = %req.symbol, "Received gRPC PlaceOrder request");

        let order = match req.into_order() {
            Ok(o) => o,
            Err(e) => {
                return Ok(Response::new(PlaceOrderResponse {
                    success: false,
                    order_id: String::new(),
                    status: proto::OrderStatus::Rejected as i32,
                    events: vec![],
                    message: format!("Invalid order: {}", e),
                }));
            }
        };

        let order_id = order.id.to_string();
        let engine = Arc::clone(&self.engine);

        // Run engine matching (in blocking pool if needed, or directly as it's memory-locked)
        let match_result = tokio::task::spawn_blocking(move || engine.submit_order(order))
            .await
            .map_err(|e| Status::internal(format!("Execution task panicked: {}", e)))?;

        match match_result {
            Ok(events) => {
                // Determine final order status from events
                let final_status = events
                    .iter()
                    .filter_map(|ev| match ev {
                        TradeEvent::OrderFilled { order, .. }
                            if order.id.to_string() == order_id =>
                        {
                            Some(proto::OrderStatus::Filled)
                        }
                        TradeEvent::OrderPartiallyFilled { order, .. }
                            if order.id.to_string() == order_id =>
                        {
                            Some(proto::OrderStatus::PartiallyFilled)
                        }
                        TradeEvent::OrderCancelled { order, .. }
                            if order.id.to_string() == order_id =>
                        {
                            Some(proto::OrderStatus::Cancelled)
                        }
                        TradeEvent::OrderRejected { order, .. }
                            if order.id.to_string() == order_id =>
                        {
                            Some(proto::OrderStatus::Rejected)
                        }
                        TradeEvent::OrderAccepted { order } if order.id.to_string() == order_id => {
                            Some(proto::OrderStatus::New)
                        }
                        _ => None,
                    })
                    .next_back()
                    .unwrap_or(proto::OrderStatus::New);

                let proto_events: Vec<TradeEventMsg> =
                    events.iter().map(TradeEventMsg::from).collect();

                Ok(Response::new(PlaceOrderResponse {
                    success: true,
                    order_id,
                    status: final_status as i32,
                    events: proto_events,
                    message: "Order processed successfully".to_string(),
                }))
            }
            Err(e) => Ok(Response::new(PlaceOrderResponse {
                success: false,
                order_id,
                status: proto::OrderStatus::Rejected as i32,
                events: vec![],
                message: e.to_string(),
            })),
        }
    }

    // ───────────────────────────────────────────────────────────────
    // Cancel Order
    // ───────────────────────────────────────────────────────────────
    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();
        let order_uuid = Uuid::parse_str(&req.order_id).map_err(|e| {
            Status::invalid_argument(format!("Invalid order_id UUID '{}': {}", req.order_id, e))
        })?;

        let symbol = req.symbol;
        let engine = Arc::clone(&self.engine);

        let cancel_result =
            tokio::task::spawn_blocking(move || engine.cancel_order(&symbol, order_uuid))
                .await
                .map_err(|e| Status::internal(format!("Execution task panicked: {}", e)))?;

        match cancel_result {
            Ok(events) => {
                let proto_events: Vec<TradeEventMsg> =
                    events.iter().map(TradeEventMsg::from).collect();

                Ok(Response::new(CancelOrderResponse {
                    success: true,
                    order_id: req.order_id,
                    status: proto::OrderStatus::Cancelled as i32,
                    events: proto_events,
                    message: "Order cancelled successfully".to_string(),
                }))
            }
            Err(e) => {
                let not_found = matches!(e, EngineError::OrderNotFound(_));
                let status_code = if not_found {
                    proto::OrderStatus::Unspecified
                } else {
                    proto::OrderStatus::Rejected
                };
                Ok(Response::new(CancelOrderResponse {
                    success: false,
                    order_id: req.order_id,
                    status: status_code as i32,
                    events: vec![],
                    message: e.to_string(),
                }))
            }
        }
    }

    // ───────────────────────────────────────────────────────────────
    // Get Order Book
    // ───────────────────────────────────────────────────────────────
    async fn get_order_book(
        &self,
        request: Request<GetOrderBookRequest>,
    ) -> Result<Response<GetOrderBookResponse>, Status> {
        let req = request.into_inner();
        let depth = if req.depth == 0 {
            20
        } else {
            req.depth as usize
        };

        match self.engine.get_snapshot(&req.symbol, depth) {
            Ok(snapshot) => Ok(Response::new(GetOrderBookResponse {
                success: true,
                snapshot: Some(OrderBookSnapshotMsg::from(&snapshot)),
                message: "Snapshot retrieved successfully".to_string(),
            })),
            Err(EngineError::BookNotFound(symbol)) => Ok(Response::new(GetOrderBookResponse {
                success: true,
                snapshot: Some(OrderBookSnapshotMsg {
                    symbol,
                    bids: vec![],
                    asks: vec![],
                    best_bid: None,
                    best_ask: None,
                    spread: None,
                    bid_depth: "0".to_string(),
                    ask_depth: "0".to_string(),
                    timestamp_unix_ms: Utc::now().timestamp_millis(),
                }),
                message: "Empty orderbook".to_string(),
            })),
            Err(e) => Ok(Response::new(GetOrderBookResponse {
                success: false,
                snapshot: None,
                message: e.to_string(),
            })),
        }
    }

    // ───────────────────────────────────────────────────────────────
    // Stream Order Book (Server-Streaming)
    // ───────────────────────────────────────────────────────────────
    async fn stream_order_book(
        &self,
        request: Request<StreamOrderBookRequest>,
    ) -> Result<Response<Self::StreamOrderBookStream>, Status> {
        let req = request.into_inner();
        let symbol = req.symbol;
        let depth = if req.depth == 0 {
            20
        } else {
            req.depth as usize
        };
        let interval_ms = if req.interval_ms == 0 {
            100
        } else {
            req.interval_ms as u64
        };

        let (tx, rx) = mpsc::channel(128);
        let engine = Arc::clone(&self.engine);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                interval.tick().await;

                let snapshot_opt = engine.get_snapshot(&symbol, depth).ok();
                let snapshot_msg = match snapshot_opt {
                    Some(ref snap) => OrderBookSnapshotMsg::from(snap),
                    None => OrderBookSnapshotMsg {
                        symbol: symbol.clone(),
                        bids: vec![],
                        asks: vec![],
                        best_bid: None,
                        best_ask: None,
                        spread: None,
                        bid_depth: "0".to_string(),
                        ask_depth: "0".to_string(),
                        timestamp_unix_ms: Utc::now().timestamp_millis(),
                    },
                };

                let update = OrderBookUpdateMsg {
                    symbol: symbol.clone(),
                    snapshot: Some(snapshot_msg),
                    timestamp_unix_ms: Utc::now().timestamp_millis(),
                };

                if tx.send(Ok(update)).await.is_err() {
                    // Client disconnected
                    debug!(symbol = %symbol, "Client disconnected from orderbook stream");
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // ───────────────────────────────────────────────────────────────
    // Stream Events (Server-Streaming)
    // ───────────────────────────────────────────────────────────────
    async fn stream_events(
        &self,
        request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let req = request.into_inner();
        let filter_symbol = req.symbol.filter(|s| !s.trim().is_empty());

        let event_rx = self.engine.event_receiver();
        let (tx, rx) = mpsc::channel(256);

        tokio::task::spawn_blocking(move || {
            while let Ok(event) = event_rx.recv() {
                // Apply symbol filter if requested
                let matches_filter = match (&filter_symbol, &event) {
                    (None, _) => true,
                    (Some(sym), TradeEvent::OrderAccepted { order }) => &order.symbol == sym,
                    (Some(sym), TradeEvent::OrderRejected { order, .. }) => &order.symbol == sym,
                    (Some(sym), TradeEvent::OrderPartiallyFilled { order, .. }) => {
                        &order.symbol == sym
                    }
                    (Some(sym), TradeEvent::OrderFilled { order, .. }) => &order.symbol == sym,
                    (Some(sym), TradeEvent::OrderCancelled { order, .. }) => &order.symbol == sym,
                    (Some(sym), TradeEvent::TradeExecuted { trade }) => &trade.symbol == sym,
                    (Some(sym), TradeEvent::StopTriggered { original_stop, .. }) => {
                        &original_stop.symbol == sym
                    }
                };

                if matches_filter {
                    let proto_msg = TradeEventMsg::from(&event);
                    if tx.blocking_send(Ok(proto_msg)).is_err() {
                        // Client disconnected
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
