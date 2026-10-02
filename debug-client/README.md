# Rust OrderBook Debug Client & Metrics Dashboard

A high-performance, real-time debug client and visualization dashboard for the **Rust OrderBook, Matching Engine & Market Data Engine**.

Designed with **minimal Node dependencies** (using purely native modern Node.js and browser APIs, plus `@grpc/grpc-js` and `@grpc/proto-loader` for the native Tonic gRPC bridge).

---

## 🚀 Quick Start

### 1. Start the Node.js Debug Client
Runs instantly with standard Node.js (v18+ / v22+ / v26+):

```bash
cd debug-client
npm install      # Installs minimal @grpc packages
node server.js
```
*(or `npm start`)*

Open your browser at **[http://localhost:3000](http://localhost:3000)**.

### 2. (Optional) Run the Tonic gRPC Order Matching Engine
In another terminal, start the Rust gRPC matching engine on port `50051`:

```bash
cargo run -p order-matching-engine
```

- In the debug client UI, switch the order routing mode to **⚡ gRPC**.
- Orders placed from the Order Entry panel are routed directly to the Rust engine's `PlaceOrder` RPC endpoint.
- Streamed trade executions from `StreamEvents` are relayed into the live trade tape and candlestick chart in real time.
- Any valid quantity amount (integers, micro-lots like `0.001`, `1.0`, `100`, etc.) is accepted without restrictive step validation.

### 3. (Optional) Run the Rust Market Data WebSocket Engine
In another terminal, start the Rust WebSocket service on port `8080`:

```bash
cargo run -p market-data-service
```

The debug client automatically detects `ws://localhost:8080`, connects, and streams live orderbook depths, candlesticks, technical indicators, and executions directly from the Rust engine!

If the Rust server is not running, the debug client seamlessly runs its **Internal Mock Engine**, continuously flowing realistic market data across all panels.

### 4. (Optional) Terminal CLI Monitor Mode
You can also run a live streaming ANSI terminal dashboard in your console:

```bash
node server.js --cli
```

---

## 🌟 Key Features & Metrics Displayed

### 1. OrderBook Depth & L2 Analytics
- **Live Bid & Ask Depth Ladder**: 10 levels of bids and asks with dynamic depth bar visualizations.
- **Best Bid / Best Ask & Spread**: Real-time spread in absolute dollar amount and basis percentage.
- **Mid-Price**: `(BestBid + BestAsk) / 2`.
- **Micro-Price (Volume-Weighted Top-of-Book)**:
  $$\text{Micro-Price} = \frac{\text{BestBid} \cdot \text{AskSize}_1 + \text{BestAsk} \cdot \text{BidSize}_1}{\text{BidSize}_1 + \text{AskSize}_1}$$
- **Order Imbalance Metric**:
  $$\text{Imbalance} = \frac{\text{BidDepth} - \text{AskDepth}}{\text{BidDepth} + \text{AskDepth}}$$
  Displayed with a bi-directional visual meter (-100% full sell pressure to +100% full buy pressure).
- **Cumulative Depth Chart**: Mini visual curve illustrating supply and demand slopes meeting at the mid-price.

### 2. Candlestick (OHLCV) Canvas Chart
- Custom high-speed HTML5 Canvas renderer with Retina/High-DPI resolution.
- Multi-timeframe support: **1s, 1m, 5m, 15m, 1h, 1d**.
- Interactive crosshairs with hover OHLCV tooltip and active price line.
- Sub-chart volume histogram with bullish/bearish color coding.
- Technical overlays:
  - **SMA 20** (Yellow)
  - **EMA 12** (Cyan)
  - **Bollinger Bands** (Magenta cloud)
  - **VWAP** (Purple dashed)

### 3. Comprehensive Technical Indicators Suite
- **Relative Strength Index (RSI 14)**: Wilder's smoothed RSI gauge with dynamic zones (Oversold < 30, Neutral 30–70, Overbought > 70).
- **MACD (12, 26, 9)**: MACD Line, Signal Line, and colored directional Histogram.
- **Bollinger Bands (20, 2)**: Upper Band, Middle SMA, Lower Band, and Bandwidth volatility % indicator.
- **Moving Averages Grid**: SMA 20, SMA 50, EMA 12, EMA 26 with trend detection.
- **VWAP & ATR (14)**: Volume-Weighted Average Price and Average True Range volatility metric.

### 4. Interactive Matching Engine & Order Entry Simulator
- **Order Types**:
  - **Limit**: FIFO price-time priority resting orders.
  - **Market**: Immediate taker sweeps.
  - **Stop-Limit**: Trigger price + limit price chain.
  - **Stop-Market**: Trigger price + market sweep.
- **Time In Force (TIF)**:
  - **GTC** (Good Till Cancel)
  - **IOC** (Immediate Or Cancel)
  - **FOK** (Fill Or Kill)
- **Pending Stop Orders Manager**: Tracks active stop orders in real time and demonstrates automated trigger execution when market price touches the trigger condition.
- **Market Shocks & Sweeps**: One-click buttons to simulate aggressive taker sweeps (3 levels deep) and liquidity shocks.

### 5. Time & Sales (Trade Tape)
- Real-time stream of executions with microsecond timestamps, buyer/seller side badges, price, and trade size.
- Animated row flashing on new incoming fills.

### 6. Protocol & Diagnostics Inspector
- Raw JSON WebSocket frame logger with type filtering (`orderbook_update`, `orderbook_enriched`, `candle_update`, `indicators_update`, `trade`, `price_update`).
- Live latency ping meter (RTT in ms).
- Message throughput counter (messages/sec).
- Configurable backpressure policies (`DropOldest`, `DropLatest`, `Throttle`).

---

## 📁 Project Structure

```
debug-client/
├── package.json          # Zero-dependency configuration
├── server.js             # HTTP static server + RFC 6455 mock WS + CLI mode
├── public/
│   ├── index.html        # Clean, dark-mode terminal layout
│   ├── styles.css        # High-performance CSS grid & styling
│   └── app.js           # Pure JS WebSocket client, canvas chart, mock generator
└── README.md             # Documentation & setup guide
```
