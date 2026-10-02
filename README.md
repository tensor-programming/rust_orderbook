# High-Performance Rust OrderBook & Real-Time Market Data Engine

A high-performance, concurrent order matching engine and market data streaming service in Rust with zero-copy event broadcasting, multi-timeframe candlestick aggregation, technical indicators, Tonic gRPC services, and a lightweight debug client.

---

## 🏛️ System Architecture

```text
                                  ┌─────────────────────────────────────────┐
                                  │          Web UI Debug Client            │
                                  │   (HTML5 Canvas, L2 Book, Indicators)   │
                                  └───────────────┬─────────────────▲───────┘
                                                  │                 │
                                  HTTP / gRPC REST│                 │ WebSocket
                                  (Order Placement│                 │ (ws://127.0.0.1:8080)
                                   & Status)      │                 │
                                                  ▼                 │
┌───────────────────────────────┐  gRPC (Port 50051)  ┌─────────────┴───────────────────┐
│     order-matching-engine     │◄────────────────────│       market-data-service       │
│  - Real Rust OrderBook        │  StreamEvents /     │  - tokio-tungstenite WebSocket  │
│  - FIFO Price-Time Priority   │  GetOrderBook       │  - Candlestick Aggregator (1s-1d)│
│  - Limit, Market, Stop orders │                     │  - Technical Indicators Engine  │
│  - Tonic gRPC (Port 50051)    │                     │  - PubSub Backpressure Engine   │
│  - Mock Order Flow Generator  │                     └─────────────────────────────────┘
└───────────────────────────────┘                                       ▲
                ▲                                                       │
                └───────────────────────────┬───────────────────────────┘
                                            │
                                 ┌──────────┴──────────┐
                                 │       shared        │
                                 │ Core domain types,  │
                                 │ traits, and errors  │
                                 └─────────────────────┘
```

---

## 🚀 Running the Services

To run the complete system with live mock data flowing through the Rust orderbook:

### 1. Order Matching Engine (gRPC Server on `0.0.0.0:50051`)
Starts the matching engine and its background mock order flow generator, feeding continuous realistic limit/market orders and stop triggers into the Rust `OrderBook`:
```bash
cargo run -p order-matching-engine
```

### 2. Market Data Service (WebSocket Server on `ws://0.0.0.0:8080`)
Connects to `order-matching-engine` via gRPC, ingests real executed trades into `CandlestickAggregator`, calculates `TechnicalIndicatorsEngine` metrics, polls L2 depth snapshots, and broadcasts them over WebSockets:
```bash
cargo run -p market-data-service
```

### 3. Debug Client & Metrics Dashboard
Start the debug client on `http://localhost:3000`:
```bash
cd debug-client
node server.js
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

- **Terminal CLI Monitor**:
  ```bash
  node server.js --cli
  ```

---

## 📊 Features & Analytics

- **Order Routing via gRPC**: Orders placed through the dashboard UI are dispatched via Tonic gRPC to `order-matching-engine`.
- **Any Quantity Accepted**: Supports integer and fractional order quantities (e.g. `1`, `0.001`, `10.5`) without browser constraint validation issues.
- **Order Types**: Limit, Market, Stop-Limit, Stop-Market, with Time-in-Force (GTC, IOC, FOK).
- **L2 OrderBook**: 10-level bid/ask depth ladder with animated depth bars and cumulative curves.
- **OrderBook Metrics**: Spread ($ and %), Mid-Price, Micro-Price (volume-weighted top-of-book), Order Imbalance ratio, Bid/Ask Depth.
- **Candlesticks (OHLCV)**: High-speed HTML5 Canvas chart supporting 1s, 1m, 5m, 15m, 1h, 1d timeframes with volume histogram and crosshairs.
- **Technical Indicators**: SMA 20, SMA 50, EMA 12, EMA 26, RSI 14 (Wilder's smoothed), MACD (12, 26, 9), Bollinger Bands (20, 2), VWAP, ATR 14.
- **Time & Sales**: Real-time tape of trade executions with millisecond timestamps and buyer/seller tags.
- **Stop Order Management**: Automatic stop order tracking and triggering as market trade prices cross stop thresholds.
- **PubSub Backpressure**: Configurable strategies (`DropOldest`, `DropLatest`, `Throttle`).

---

## 🧪 Testing

Run all unit, integration, and property tests:
```bash
cargo test --workspace
```
