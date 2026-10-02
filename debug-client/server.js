#!/usr/bin/env node
/**
 * Debug Client Server for Rust OrderBook with gRPC Integration.
 * 
 * Features:
 * - Built-in HTTP static server for the trading dashboard UI (port 3000)
 * - Full Tonic gRPC Server Integration (OrderMatchingService on port 50051)
 *   - PlaceOrder (Limit, Market, Stop-Limit, Stop-Market, GTC/IOC/FOK)
 *   - CancelOrder
 *   - GetOrderBook
 *   - StreamOrderBook
 *   - StreamEvents (Real-time engine event forwarding)
 * - Built-in RFC-6455 WebSocket mock server on /ws (zero npm dependencies)
 * - Live terminal CLI dashboard mode (`node server.js --cli`)
 */

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const HTTP_PORT = parseInt(process.env.PORT || '3000', 10);
const RUST_WS_PORT = parseInt(process.env.WS_PORT || '8080', 10);
const RUST_WS_HOST = process.env.WS_HOST || '127.0.0.1';
const GRPC_PORT = parseInt(process.env.GRPC_PORT || '50051', 10);
const GRPC_HOST = process.env.GRPC_HOST || '127.0.0.1';
const PUBLIC_DIR = path.join(__dirname, 'public');
const PROTO_PATH = path.join(__dirname, '..', 'proto', 'order_matching.proto');

// Optional gRPC imports (graceful fallback if not installed)
let grpc = null;
let protoLoader = null;
try {
  grpc = require('@grpc/grpc-js');
  protoLoader = require('@grpc/proto-loader');
} catch (e) {
  console.warn('[gRPC] Optional @grpc/grpc-js or @grpc/proto-loader not available. Run npm install if gRPC is needed.');
}

// MIME types for static assets
const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.png': 'image/png',
  '.txt': 'text/plain; charset=utf-8',
};

// ═══════════════════════════════════════════════════════════════════
// CLI Mode (Terminal Dashboard)
// ═══════════════════════════════════════════════════════════════════

if (process.argv.includes('--cli')) {
  runCliMode();
} else if (process.argv.includes('--test')) {
  console.log('[Test] Server module loaded successfully.');
  process.exit(0);
} else {
  runServer();
}

// ═══════════════════════════════════════════════════════════════════
// gRPC Client Integration (OrderMatchingService)
// ═══════════════════════════════════════════════════════════════════

let grpcClient = null;
let isGrpcConnected = false;
let grpcEventsStream = null;

function initGrpcClient() {
  if (!grpc || !protoLoader) return;
  if (!fs.existsSync(PROTO_PATH)) {
    console.warn(`[gRPC] Proto file not found at ${PROTO_PATH}`);
    return;
  }

  try {
    const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });
    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    const orderMatching = protoDescriptor.order_matching;

    grpcClient = new orderMatching.OrderMatchingService(
      `${GRPC_HOST}:${GRPC_PORT}`,
      grpc.credentials.createInsecure()
    );

    console.log(`  🦀 gRPC Target:        ${GRPC_HOST}:${GRPC_PORT} (OrderMatchingService)`);
    checkGrpcConnectivity();
  } catch (err) {
    console.error('[gRPC] Initialization error:', err.message);
  }
}

function checkGrpcConnectivity() {
  if (!grpcClient) return;

  grpcClient.GetOrderBook({ symbol: 'BTC-USD', depth: 5 }, (err, response) => {
    const wasConnected = isGrpcConnected;
    if (err) {
      isGrpcConnected = false;
      if (wasConnected) {
        console.log(`[gRPC] Connection lost to ${GRPC_HOST}:${GRPC_PORT}`);
      }
      setTimeout(checkGrpcConnectivity, 3000);
    } else {
      isGrpcConnected = true;
      if (!wasConnected) {
        console.log(`[gRPC] Connected to OrderMatchingService on ${GRPC_HOST}:${GRPC_PORT}`);
        startGrpcEventStreaming();
      }
      setTimeout(checkGrpcConnectivity, 5000);
    }

    // Broadcast status update
    broadcastWsMessage({
      type: 'grpc_status',
      connected: isGrpcConnected,
      host: GRPC_HOST,
      port: GRPC_PORT,
    });
  });
}

function startGrpcEventStreaming() {
  if (!grpcClient || !isGrpcConnected) return;
  if (grpcEventsStream) {
    try { grpcEventsStream.cancel(); } catch (e) {}
  }

  try {
    grpcEventsStream = grpcClient.StreamEvents({});
    grpcEventsStream.on('data', (eventMsg) => {
      // Forward raw gRPC trade event to all WebSocket clients
      broadcastWsMessage({
        type: 'grpc_event',
        event: eventMsg,
        timestamp: Date.now(),
      });

      // If a trade occurred, also propagate as trade & price update
      if (eventMsg.trade_executed && eventMsg.trade_executed.trade) {
        const trade = eventMsg.trade_executed.trade;
        const isMakerBuy = trade.maker_side === 'BUY' || trade.maker_side === 1;
        const takerSide = isMakerBuy ? 'sell' : 'buy';

        broadcastWsMessage({
          type: 'trade',
          id: trade.id,
          symbol: trade.symbol,
          price: trade.price,
          size: trade.quantity,
          side: takerSide,
          timestamp: Number(trade.created_at_unix_ms) || Date.now(),
        }, trade.symbol);

        broadcastWsMessage({
          type: 'price_update',
          symbol: trade.symbol,
          price: trade.price,
          timestamp: Number(trade.created_at_unix_ms) || Date.now(),
        }, trade.symbol);
      }
    });

    grpcEventsStream.on('error', (err) => {
      grpcEventsStream = null;
    });

    grpcEventsStream.on('end', () => {
      grpcEventsStream = null;
    });
  } catch (err) {
    console.warn('[gRPC] Error starting event stream:', err.message);
  }
}

function normalizeOrderPayload(body) {
  if (!body) return {};
  const reqPayload = {
    symbol: body.symbol || 'BTC-USD',
    side: body.side || 'BUY',
    order_type: body.order_type || 'LIMIT',
    quantity: String(body.quantity !== undefined ? body.quantity : '1'),
    time_in_force: body.time_in_force || 'GOOD_TILL_CANCEL',
  };
  if (body.price !== undefined && body.price !== null && String(body.price).trim() !== '') {
    reqPayload.price = String(body.price);
  }
  if (body.stop_price !== undefined && body.stop_price !== null && String(body.stop_price).trim() !== '') {
    reqPayload.stop_price = String(body.stop_price);
  }
  if (body.custom_order_id) {
    reqPayload.custom_order_id = String(body.custom_order_id);
  }
  return reqPayload;
}

// ═══════════════════════════════════════════════════════════════════
// HTTP Static File Server & API Endpoints
// ═══════════════════════════════════════════════════════════════════

function runServer() {
  const server = http.createServer((req, res) => {
    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    let pathname = decodeURIComponent(url.pathname);

    // Helper for reading JSON body
    function readBody(callback) {
      let body = '';
      req.on('data', chunk => { body += chunk; });
      req.on('end', () => {
        try {
          const parsed = body ? JSON.parse(body) : {};
          callback(null, parsed);
        } catch (e) {
          callback(e);
        }
      });
    }

    // ─────────────────────────────────────────────────────────────
    // API Endpoints
    // ─────────────────────────────────────────────────────────────

    if (pathname === '/api/status') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'online',
        httpPort: HTTP_PORT,
        rustWsTarget: `ws://${RUST_WS_HOST}:${RUST_WS_PORT}`,
        grpcTarget: `${GRPC_HOST}:${GRPC_PORT}`,
        grpcConnected: isGrpcConnected,
        activeWsClients: wsClients.size,
        uptimeSeconds: Math.floor(process.uptime()),
        timestamp: Date.now(),
      }));
      return;
    }

    if (pathname === '/api/grpc/status') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        connected: isGrpcConnected,
        host: GRPC_HOST,
        port: GRPC_PORT,
      }));
      return;
    }

    // gRPC PlaceOrder endpoint
    if (pathname === '/api/grpc/place_order' && req.method === 'POST') {
      readBody((err, body) => {
        if (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, message: 'Invalid JSON payload' }));
          return;
        }

        if (!grpcClient || !isGrpcConnected) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            success: false,
            message: `gRPC OrderMatchingService at ${GRPC_HOST}:${GRPC_PORT} is currently offline. Please ensure 'cargo run -p order-matching-engine' is running.`,
          }));
          return;
        }

        const normalized = normalizeOrderPayload(body);
        grpcClient.PlaceOrder(normalized, (grpcErr, response) => {
          if (grpcErr) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
              success: false,
              message: grpcErr.details || grpcErr.message,
            }));
          } else {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(response));
          }
        });
      });
      return;
    }

    // gRPC CancelOrder endpoint
    if (pathname === '/api/grpc/cancel_order' && req.method === 'POST') {
      readBody((err, body) => {
        if (err || !body.symbol || !body.order_id) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, message: 'Missing symbol or order_id' }));
          return;
        }

        if (!grpcClient || !isGrpcConnected) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            success: false,
            message: `gRPC OrderMatchingService at ${GRPC_HOST}:${GRPC_PORT} is currently offline.`,
          }));
          return;
        }

        grpcClient.CancelOrder({ symbol: body.symbol, order_id: body.order_id }, (grpcErr, response) => {
          if (grpcErr) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
              success: false,
              message: grpcErr.details || grpcErr.message,
            }));
          } else {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(response));
          }
        });
      });
      return;
    }

    // gRPC GetOrderBook endpoint
    if (pathname === '/api/grpc/orderbook' && req.method === 'GET') {
      const symbol = url.searchParams.get('symbol') || 'BTC-USD';
      const depth = parseInt(url.searchParams.get('depth') || '20', 10);

      if (!grpcClient || !isGrpcConnected) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, message: 'gRPC server offline' }));
        return;
      }

      grpcClient.GetOrderBook({ symbol, depth }, (grpcErr, response) => {
        if (grpcErr) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, message: grpcErr.message }));
        } else {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(response));
        }
      });
      return;
    }

    // ─────────────────────────────────────────────────────────────
    // Static File Routing
    // ─────────────────────────────────────────────────────────────

    if (pathname === '/') pathname = '/index.html';
    const filePath = path.normalize(path.join(PUBLIC_DIR, pathname));

    if (!filePath.startsWith(PUBLIC_DIR)) {
      res.writeHead(403, { 'Content-Type': 'text/plain' });
      res.end('403 Forbidden');
      return;
    }

    fs.stat(filePath, (err, stats) => {
      if (err || !stats.isFile()) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('404 Not Found');
        return;
      }

      const ext = path.extname(filePath).toLowerCase();
      const contentType = MIME_TYPES[ext] || 'application/octet-stream';

      res.writeHead(200, {
        'Content-Type': contentType,
        'Content-Length': stats.size,
        'Cache-Control': 'no-cache',
      });

      const stream = fs.createReadStream(filePath);
      stream.pipe(res);
    });
  });

  // Upgrade HTTP to RFC-6455 WebSocket
  server.on('upgrade', (req, socket) => {
    handleWebSocketUpgrade(req, socket);
  });

  server.listen(HTTP_PORT, () => {
    console.log('\n' + '='.repeat(68));
    console.log('  RUST ORDERBOOK DEBUG CLIENT & gRPC INTEGRATION DASHBOARD');
    console.log('='.repeat(68));
    console.log(`  🌐 Web UI Dashboard:   http://localhost:${HTTP_PORT}`);
    console.log(`  ⚡ Mock WebSocket:     ws://localhost:${HTTP_PORT}/ws`);
    console.log(`  🦀 Rust Market WS:     ws://${RUST_WS_HOST}:${RUST_WS_PORT}`);
    initGrpcClient();
    console.log('='.repeat(68));
    console.log('  Navigate to http://localhost:' + HTTP_PORT);
    console.log('  Press Ctrl+C to stop.\n');
  });

  // Stream real OrderBook depth from gRPC to local WS clients
  startGrpcOrderBookPoller();
}

// ═══════════════════════════════════════════════════════════════════
// Native RFC-6455 WebSocket Server Implementation
// ═══════════════════════════════════════════════════════════════════

const wsClients = new Set();
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function handleWebSocketUpgrade(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.destroy();
    return;
  }

  const acceptKey = crypto
    .createHash('sha1')
    .update(key + WS_GUID)
    .digest('base64');

  const headers = [
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${acceptKey}`,
    '\r\n',
  ];

  socket.write(headers.join('\r\n'));

  const client = {
    socket,
    subscriptions: new Set(['*']),
    isAlive: true,
  };
  wsClients.add(client);

  sendWsJson(client, {
    type: 'connected',
    message: 'Connected to Market Data Server with gRPC integration',
    grpcConnected: isGrpcConnected,
    grpcTarget: `${GRPC_HOST}:${GRPC_PORT}`,
  });

  socket.on('data', (buffer) => {
    handleWsData(client, buffer);
  });

  socket.on('close', () => {
    wsClients.delete(client);
  });

  socket.on('error', () => {
    wsClients.delete(client);
  });
}

function handleWsData(client, buffer) {
  if (buffer.length < 2) return;

  const firstByte = buffer[0];
  const secondByte = buffer[1];
  const opcode = firstByte & 0x0f;
  const isMasked = (secondByte & 0x80) !== 0;

  if (opcode === 0x08) {
    client.socket.end();
    wsClients.delete(client);
    return;
  }

  if (opcode === 0x09) {
    sendWsPong(client);
    return;
  }

  let payloadLength = secondByte & 0x7f;
  let offset = 2;

  if (payloadLength === 126) {
    payloadLength = buffer.readUInt16BE(offset);
    offset += 2;
  } else if (payloadLength === 127) {
    payloadLength = Number(buffer.readBigUInt64BE(offset));
    offset += 8;
  }

  let maskKey = null;
  if (isMasked) {
    maskKey = buffer.slice(offset, offset + 4);
    offset += 4;
  }

  const payload = buffer.slice(offset, offset + payloadLength);
  if (isMasked && maskKey) {
    for (let i = 0; i < payload.length; i++) {
      payload[i] ^= maskKey[i % 4];
    }
  }

  try {
    const text = payload.toString('utf-8');
    const msg = JSON.parse(text);

    if (msg.type === 'subscribe') {
      const target = (msg.symbol || msg.market_id || '*').toUpperCase();
      client.subscriptions.add(target);
      sendWsJson(client, {
        type: 'subscribed',
        symbol: msg.symbol,
        channel: msg.channel || 'all',
        timeframe: msg.timeframe,
      });
    } else if (msg.type === 'unsubscribe') {
      const target = (msg.symbol || msg.market_id || '*').toUpperCase();
      client.subscriptions.delete(target);
      sendWsJson(client, {
        type: 'unsubscribed',
        symbol: msg.symbol,
        channel: msg.channel || 'all',
      });
    } else if (msg.type === 'ping') {
      sendWsJson(client, {
        type: 'pong',
        timestamp: Date.now(),
      });
    } else if (msg.type === 'grpc_place_order') {
      // Forward directly to gRPC
      if (grpcClient && isGrpcConnected) {
        const normalized = normalizeOrderPayload(msg.order);
        grpcClient.PlaceOrder(normalized, (err, response) => {
          sendWsJson(client, {
            type: 'grpc_place_order_response',
            success: !err && response.success,
            response: response || null,
            error: err ? (err.details || err.message) : null,
          });
        });
      } else {
        sendWsJson(client, {
          type: 'grpc_place_order_response',
          success: false,
          error: `gRPC OrderMatchingService at ${GRPC_HOST}:${GRPC_PORT} is offline.`,
        });
      }
    } else if (msg.type === 'grpc_cancel_order') {
      if (grpcClient && isGrpcConnected) {
        grpcClient.CancelOrder({ symbol: msg.symbol, order_id: msg.order_id }, (err, response) => {
          sendWsJson(client, {
            type: 'grpc_cancel_order_response',
            success: !err && response.success,
            response: response || null,
            error: err ? (err.details || err.message) : null,
          });
        });
      } else {
        sendWsJson(client, {
          type: 'grpc_cancel_order_response',
          success: false,
          error: `gRPC OrderMatchingService at ${GRPC_HOST}:${GRPC_PORT} is offline.`,
        });
      }
    }
  } catch (e) {}
}

function sendWsJson(client, obj) {
  const json = JSON.stringify(obj);
  const payload = Buffer.from(json, 'utf-8');
  const frame = encodeWsFrame(payload, 0x01);
  try {
    client.socket.write(frame);
  } catch (e) {
    wsClients.delete(client);
  }
}

function sendWsPong(client) {
  const frame = encodeWsFrame(Buffer.alloc(0), 0x0a);
  try {
    client.socket.write(frame);
  } catch (e) {
    wsClients.delete(client);
  }
}

function encodeWsFrame(payload, opcode) {
  const len = payload.length;
  let header;

  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | (opcode & 0x0f);
    header[1] = len;
  } else if (len <= 65535) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | (opcode & 0x0f);
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | (opcode & 0x0f);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }

  return Buffer.concat([header, payload]);
}

function broadcastWsMessage(msg, symbol) {
  if (wsClients.size === 0) return;
  const json = JSON.stringify(msg);
  const payload = Buffer.from(json, 'utf-8');
  const frame = encodeWsFrame(payload, 0x01);

  const symUpper = (symbol || '*').toUpperCase();
  for (const client of wsClients) {
    if (client.subscriptions.has('*') || client.subscriptions.has(symUpper)) {
      try {
        client.socket.write(frame);
      } catch (e) {
        wsClients.delete(client);
      }
    }
  }
}

// ═══════════════════════════════════════════════════════════════════
// Real Rust OrderBook Poller & Broadcaster (via gRPC)
// ═══════════════════════════════════════════════════════════════════

function startGrpcOrderBookPoller() {
  const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD'];
  let currentIdx = 0;

  setInterval(() => {
    if (!grpcClient || !isGrpcConnected || wsClients.size === 0) return;

    const symbol = symbols[currentIdx % symbols.length];
    currentIdx++;

    grpcClient.GetOrderBook({ symbol, depth: 20 }, (err, response) => {
      if (err || !response || !response.snapshot) return;

      const snap = response.snapshot;
      const bids = (snap.bids || []).map((b) => ({ price: b.price, size: b.quantity || b.total_quantity || '0' }));
      const asks = (snap.asks || []).map((a) => ({ price: a.price, size: a.quantity || a.total_quantity || '0' }));

      const now = Date.now();
      const marketId = symbol === 'BTC-USD' ? '1' : symbol === 'ETH-USD' ? '2' : '3';

      broadcastWsMessage({
        type: 'orderbook_update',
        symbol,
        marketId,
        bids,
        asks,
        timestamp: now,
      }, symbol);

      if (bids.length > 0 && asks.length > 0) {
        const bestBid = +bids[0].price;
        const bestAsk = +asks[0].price;
        const spread = +(bestAsk - bestBid).toFixed(2);
        const spreadPct = bestAsk > 0 ? +((spread / bestAsk) * 100).toFixed(4) : 0;
        const midPrice = +((bestBid + bestAsk) / 2).toFixed(2);
        const bidDepth = bids.reduce((acc, b) => acc + (+b.size || 0), 0);
        const askDepth = asks.reduce((acc, a) => acc + (+a.size || 0), 0);
        const totalDepth = bidDepth + askDepth;
        const orderImbalance = totalDepth > 0 ? +((bidDepth - askDepth) / totalDepth).toFixed(4) : 0;
        const l1BidSz = +bids[0].size || 1;
        const l1AskSz = +asks[0].size || 1;
        const microPrice = +(((bestBid * l1AskSz) + (bestAsk * l1BidSz)) / (l1BidSz + l1AskSz)).toFixed(2);

        broadcastWsMessage({
          type: 'orderbook_enriched',
          snapshot: {
            symbol,
            marketId,
            bids,
            asks,
            bestBid: bestBid.toString(),
            bestAsk: bestAsk.toString(),
            spread: spread.toString(),
            spreadPercentage: spreadPct.toString(),
            midPrice: midPrice.toString(),
            microPrice: microPrice.toString(),
            bidDepth: bidDepth.toFixed(4),
            askDepth: askDepth.toFixed(4),
            orderImbalance: orderImbalance.toString(),
            timestamp: now,
          },
        }, symbol);
      }
    });
  }, 250);
}

// ═══════════════════════════════════════════════════════════════════
// CLI Terminal Dashboard Mode
// ═══════════════════════════════════════════════════════════════════

function runCliMode() {
  console.clear();
  console.log('\x1b[1;36m[Rust OrderBook CLI Monitor]\x1b[0m Connecting to ws://' + RUST_WS_HOST + ':' + RUST_WS_PORT + '...');

  let ws;
  let isConnected = false;
  let lastPrice = '0';
  let bestBid = '0';
  let bestAsk = '0';
  let spread = '0';
  let microPrice = '0';
  let imbalance = '0';
  let tradeCount = 0;
  let topBids = [];
  let topAsks = [];
  let rsi = '50';
  let vwap = '0';
  let orderSecCounter = 0;
  let tradeSecCounter = 0;
  let ordersPerSec = 0;
  let tradesPerSec = 0;

  setInterval(() => {
    ordersPerSec = orderSecCounter;
    tradesPerSec = tradeSecCounter;
    orderSecCounter = 0;
    tradeSecCounter = 0;
    if (isConnected) renderTerminal();
  }, 1000);

  function renderTerminal() {
    console.clear();
    console.log('\x1b[1;35m══════════════════════════════════════════════════════════════════════\x1b[0m');
    console.log('\x1b[1;37m   RUST ORDERBOOK LIVE DEBUG MONITOR   \x1b[0m\x1b[32m[● ' + (isConnected ? 'CONNECTED' : 'WAITING FOR RUST WS...') + ']\x1b[0m');
    console.log('\x1b[1;35m══════════════════════════════════════════════════════════════════════\x1b[0m');

    console.log(`\x1b[1;33m Symbol:\x1b[0m BTC-USD        \x1b[1;33mLast Price:\x1b[0m \x1b[1;32m$${lastPrice}\x1b[0m    \x1b[1;33mOrders/s:\x1b[0m \x1b[1;36m${ordersPerSec}\x1b[0m    \x1b[1;33mTrades/s:\x1b[0m \x1b[1;32m${tradesPerSec}\x1b[0m`);
    console.log(`\x1b[1;33m Best Bid:\x1b[0m \x1b[32m$${bestBid}\x1b[0m   \x1b[1;33mBest Ask:\x1b[0m \x1b[31m$${bestAsk}\x1b[0m    \x1b[1;33mSpread:\x1b[0m $${spread}    \x1b[1;33mTrades:\x1b[0m ${tradeCount}`);
    console.log(`\x1b[1;33m Mid Price:\x1b[0m $${((+bestBid + +bestAsk)/2).toFixed(2)}  \x1b[1;33mMicro-Price:\x1b[0m $${microPrice}  \x1b[1;33mImbalance:\x1b[0m ${(Number(imbalance)*100).toFixed(1)}%`);
    console.log(`\x1b[1;33m RSI (14):\x1b[0m ${rsi}           \x1b[1;33mVWAP:\x1b[0m $${vwap}`);
    console.log('\x1b[1;34m──────────────────────────────────────────────────────────────────────\x1b[0m');
    console.log(' \x1b[1mASKS (Top 5)\x1b[0m');
    topAsks.slice(0, 5).reverse().forEach(a => {
      console.log(`   \x1b[31m${(+a.price).toFixed(2).padStart(10)}\x1b[0m  |  ${(+a.size).toFixed(4).padStart(8)}`);
    });
    console.log(' \x1b[1m----------------- SPREAD: $' + spread + ' -----------------\x1b[0m');
    console.log(' \x1b[1mBIDS (Top 5)\x1b[0m');
    topBids.slice(0, 5).forEach(b => {
      console.log(`   \x1b[32m${(+b.price).toFixed(2).padStart(10)}\x1b[0m  |  ${(+b.size).toFixed(4).padStart(8)}`);
    });
    console.log('\x1b[1;35m══════════════════════════════════════════════════════════════════════\x1b[0m');
    console.log(' Press Ctrl+C to exit CLI monitor.');
  }

  function connectWs() {
    if (typeof globalThis.WebSocket === 'undefined') {
      console.log('Native WebSocket not supported in this Node environment.');
      return;
    }

    try {
      ws = new globalThis.WebSocket(`ws://${RUST_WS_HOST}:${RUST_WS_PORT}`);
      ws.onopen = () => {
        isConnected = true;
        ws.send(JSON.stringify({ type: 'subscribe', symbol: 'BTC-USD', channel: 'all' }));
        renderTerminal();
      };
      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          handleIncomingMessage(msg);
        } catch (e) {}
      };
      ws.onerror = () => {
        // Handled in onclose
      };
      ws.onclose = () => {
        isConnected = false;
        renderTerminal();
        setTimeout(connectWs, 2000);
      };
    } catch (e) {
      isConnected = false;
      renderTerminal();
      setTimeout(connectWs, 2000);
    }
  }

  connectWs();

  function handleIncomingMessage(msg) {
    if (msg.type === 'price_update') {
      lastPrice = (+msg.price).toFixed(2);
    } else if (msg.type === 'trade') {
      tradeCount++;
      tradeSecCounter++;
      orderSecCounter += 2;
      lastPrice = (+msg.price).toFixed(2);
    } else if (msg.type === 'orderbook_update') {
      orderSecCounter++;
    } else if (msg.type === 'orderbook_enriched' && msg.snapshot) {
      orderSecCounter++;
      bestBid = (+msg.snapshot.bestBid || 0).toFixed(2);
      bestAsk = (+msg.snapshot.bestAsk || 0).toFixed(2);
      spread = (+msg.snapshot.spread || 0).toFixed(2);
      microPrice = (+msg.snapshot.microPrice || 0).toFixed(2);
      imbalance = msg.snapshot.orderImbalance || '0';
      topBids = msg.snapshot.bids || [];
      topAsks = msg.snapshot.asks || [];
    } else if (msg.type === 'indicators_update' && msg.indicators) {
      rsi = (+msg.indicators.rsi_14 || 50).toFixed(1);
      vwap = (+msg.indicators.vwap || 0).toFixed(2);
    }
    renderTerminal();
  }
}
