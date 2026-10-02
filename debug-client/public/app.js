/**
 * Rust OrderBook Debug Client - Core Application Logic
 * Zero-dependency, pure vanilla modern JavaScript (ES2022)
 */

(function () {
  'use strict';

  // ═══════════════════════════════════════════════════════════════════
  // Application State
  // ═══════════════════════════════════════════════════════════════════

  const state = {
    symbol: 'BTC-USD',
    marketId: '1',
    timeframe: '1s',
    sourceMode: 'live', // 'live' | 'mock'
    simRunning: true,
    simSpeed: 1.0,
    backpressure: 'drop_oldest',
    orderRouting: 'grpc', // 'grpc' | 'local'
    grpcConnected: false,

    // Live orderbook data
    orderbook: {
      bids: [],
      asks: [],
      bestBid: 0,
      bestAsk: 0,
      spread: 0,
      spreadPct: 0,
      midPrice: 0,
      microPrice: 0,
      bidDepth: 0,
      askDepth: 0,
      orderImbalance: 0,
      timestamp: Date.now(),
    },

    // Price ticker
    price: 0,
    prevPrice: 0,
    open24h: 52000,
    tradeCount: 0,
    volume24h: 0,

    // Candlesticks map: `${symbol}:${timeframe}` -> Candlestick[]
    candles: new Map(),

    // Technical indicators
    indicators: {
      sma_20: null,
      sma_50: null,
      ema_12: null,
      ema_26: null,
      rsi_14: 50,
      macd: { macdLine: 0, signalLine: 0, histogram: 0 },
      bollinger_bands: { upper: 0, middle: 0, lower: 0, bandwidth: 0 },
      vwap: 0,
      atr_14: 0,
    },

    // Trades (Time & Sales)
    trades: [],

    // Pending Stop Orders
    pendingStops: [],

    // Protocol log
    frameLogs: [],
    maxFrameLogs: 100,
    isFrameLogPaused: false,
    frameFilter: 'all',

    // WebSocket connection state
    ws: null,
    wsConnected: false,
    reconnectTimer: null,
    pingTimer: null,
    lastPingTime: 0,
    latency: null,
    msgCounter: 0,
    msgRate: 0,
    orderCounter: 0,
    tradeCounter: 0,
    ordersPerSec: 0,
    tradesPerSec: 0,
    rateInterval: null,
  };

  // DOM Elements Cache
  const DOM = {};

  // ═══════════════════════════════════════════════════════════════════
  // Initialization
  // ═══════════════════════════════════════════════════════════════════

  window.addEventListener('DOMContentLoaded', () => {
    cacheDomElements();
    initSymbolData();
    setupEventListeners();
    setupCanvasChart();
    startThroughputCounter();

    // Default to connecting to Live Engine (ws://localhost:8080)
    connectWebSocket();

    // Start UI animation loop
    requestAnimationFrame(renderLoop);
  });

  function cacheDomElements() {
    DOM.symbolBtns = document.querySelectorAll('.symbol-btn');
    DOM.btnSourceLive = document.getElementById('btnSourceLive');
    DOM.btnSourceMock = document.getElementById('btnSourceMock');
    DOM.btnPlayPause = document.getElementById('btnPlayPause');
    DOM.simSpeedSelect = document.getElementById('simSpeedSelect');
    DOM.btnSweepBuy = document.getElementById('btnSweepBuy');
    DOM.btnSweepSell = document.getElementById('btnSweepSell');
    DOM.btnFlashCrash = document.getElementById('btnFlashCrash');

    DOM.statusDot = document.getElementById('statusDot');
    DOM.statusText = document.getElementById('statusText');
    DOM.latencyValue = document.getElementById('latencyValue');
    DOM.msgRateValue = document.getElementById('msgRateValue');
    DOM.ordersPerSec = document.getElementById('ordersPerSec');
    DOM.tradesPerSec = document.getElementById('tradesPerSec');
    DOM.tickerOrdersPerSec = document.getElementById('tickerOrdersPerSec');
    DOM.tickerTradesPerSec = document.getElementById('tickerTradesPerSec');
    DOM.bpStrategyDisplay = document.getElementById('bpStrategyDisplay');

    // Ticker elements
    DOM.tickerPrice = document.getElementById('tickerPrice');
    DOM.tickerChange = document.getElementById('tickerChange');
    DOM.tickerBestBid = document.getElementById('tickerBestBid');
    DOM.tickerBestAsk = document.getElementById('tickerBestAsk');
    DOM.tickerSpread = document.getElementById('tickerSpread');
    DOM.tickerSpreadPct = document.getElementById('tickerSpreadPct');
    DOM.tickerMidPrice = document.getElementById('tickerMidPrice');
    DOM.tickerMicroPrice = document.getElementById('tickerMicroPrice');
    DOM.tickerImbalanceBar = document.getElementById('tickerImbalanceBar');
    DOM.tickerImbalanceVal = document.getElementById('tickerImbalanceVal');
    DOM.tickerBidDepth = document.getElementById('tickerBidDepth');
    DOM.tickerAskDepth = document.getElementById('tickerAskDepth');
    DOM.tickerVwap = document.getElementById('tickerVwap');
    DOM.ticker24hVol = document.getElementById('ticker24hVol');
    DOM.tickerTradeCount = document.getElementById('tickerTradeCount');

    // OrderBook elements
    DOM.asksContainer = document.getElementById('asksContainer');
    DOM.bidsContainer = document.getElementById('bidsContainer');
    DOM.spreadRowAmount = document.getElementById('spreadRowAmount');
    DOM.spreadRowPercent = document.getElementById('spreadRowPercent');
    DOM.spreadRowMid = document.getElementById('spreadRowMid');
    DOM.imbalanceStatusPill = document.getElementById('imbalanceStatusPill');
    DOM.detailedBidBar = document.getElementById('detailedBidBar');
    DOM.detailedAskBar = document.getElementById('detailedAskBar');
    DOM.detailedBidPct = document.getElementById('detailedBidPct');
    DOM.detailedAskPct = document.getElementById('detailedAskPct');
    DOM.depthMiniCanvas = document.getElementById('depthMiniCanvas');

    // Chart elements
    DOM.candlestickCanvas = document.getElementById('candlestickCanvas');
    DOM.canvasWrapper = document.getElementById('canvasWrapper');
    DOM.timeframeBtns = document.querySelectorAll('.tf-btn');
    DOM.chkSma20 = document.getElementById('chkSma20');
    DOM.chkEma12 = document.getElementById('chkEma12');
    DOM.chkBollinger = document.getElementById('chkBollinger');
    DOM.chkVwap = document.getElementById('chkVwap');
    DOM.hoverO = document.getElementById('hoverO');
    DOM.hoverH = document.getElementById('hoverH');
    DOM.hoverL = document.getElementById('hoverL');
    DOM.hoverC = document.getElementById('hoverC');
    DOM.hoverV = document.getElementById('hoverV');

    // Indicators elements
    DOM.rsiValue = document.getElementById('rsiValue');
    DOM.rsiPin = document.getElementById('rsiPin');
    DOM.rsiPill = document.getElementById('rsiPill');
    DOM.macdLineVal = document.getElementById('macdLineVal');
    DOM.macdSignalVal = document.getElementById('macdSignalVal');
    DOM.macdHistVal = document.getElementById('macdHistVal');
    DOM.macdHistBar = document.getElementById('macdHistBar');
    DOM.macdPill = document.getElementById('macdPill');
    DOM.bbUpperVal = document.getElementById('bbUpperVal');
    DOM.bbMiddleVal = document.getElementById('bbMiddleVal');
    DOM.bbLowerVal = document.getElementById('bbLowerVal');
    DOM.bbWidthVal = document.getElementById('bbWidthVal');
    DOM.bbPill = document.getElementById('bbPill');
    DOM.sma20Val = document.getElementById('sma20Val');
    DOM.sma50Val = document.getElementById('sma50Val');
    DOM.ema12Val = document.getElementById('ema12Val');
    DOM.ema26Val = document.getElementById('ema26Val');
    DOM.atrVal = document.getElementById('atrVal');
    DOM.maTrendPill = document.getElementById('maTrendPill');

    // Order entry elements
    DOM.orderForm = document.getElementById('orderForm');
    DOM.tabBuy = document.getElementById('tabBuy');
    DOM.tabSell = document.getElementById('tabSell');
    DOM.orderType = document.getElementById('orderType');
    DOM.timeInForce = document.getElementById('timeInForce');
    DOM.rowLimitPrice = document.getElementById('rowLimitPrice');
    DOM.rowStopPrice = document.getElementById('rowStopPrice');
    DOM.orderPrice = document.getElementById('orderPrice');
    DOM.orderStopPrice = document.getElementById('orderStopPrice');
    DOM.orderQuantity = document.getElementById('orderQuantity');
    DOM.btnSubmitOrder = document.getElementById('btnSubmitOrder');
    DOM.orderFeedback = document.getElementById('orderFeedback');
    DOM.feedbackText = document.getElementById('feedbackText');
    DOM.btnRouteGrpc = document.getElementById('btnRouteGrpc');
    DOM.btnRouteLocal = document.getElementById('btnRouteLocal');
    DOM.grpcStatusDot = document.getElementById('grpcStatusDot');
    DOM.grpcStatusText = document.getElementById('grpcStatusText');
    DOM.stopOrdersList = document.getElementById('stopOrdersList');
    DOM.stopOrderCount = document.getElementById('stopOrderCount');
    DOM.tradeTapeList = document.getElementById('tradeTapeList');
    DOM.tradeTapeCount = document.getElementById('tradeTapeCount');

    // Footer tabs & inspector
    DOM.footerTabs = document.querySelectorAll('.footer-tab-btn');
    DOM.tabPanes = document.querySelectorAll('.tab-pane');
    DOM.framesTerminal = document.getElementById('framesTerminal');
    DOM.filterPills = document.querySelectorAll('.filter-pill');
    DOM.btnPauseFrames = document.getElementById('btnPauseFrames');
    DOM.btnClearFrames = document.getElementById('btnClearFrames');
    DOM.btnSendPing = document.getElementById('btnSendPing');
    DOM.btnApplySubscriptions = document.getElementById('btnApplySubscriptions');
    DOM.btnReconnectWs = document.getElementById('btnReconnectWs');
    DOM.inputRustWsUrl = document.getElementById('inputRustWsUrl');
    DOM.inputNodeWsUrl = document.getElementById('inputNodeWsUrl');
  }

  // ═══════════════════════════════════════════════════════════════════
  // Initial Mock Seed Data Generation
  // ═══════════════════════════════════════════════════════════════════

  const TIMEFRAMES = ['1s', '1m', '5m', '15m', '1h', '1d'];

  function initSymbolData() {
    const basePrices = {
      'BTC-USD': 52345.50,
      'ETH-USD': 3125.20,
      'SOL-USD': 145.80,
    };
    const p = basePrices[state.symbol] || 50000;
    state.price = p;
    state.prevPrice = p;
    state.open24h = p * 0.985;
    DOM.orderPrice.value = p.toFixed(2);

    // Pre-populate continuous historical candles for all timeframes so charts & indicators look rich
    state.candles.clear();
    TIMEFRAMES.forEach((tf) => ensureCandlesForTimeframe(tf));

    computeLocalIndicators();
    fetchInitialOrderBook(state.symbol);
  }

  async function fetchInitialOrderBook(symbol) {
    try {
      const res = await fetch(`/api/grpc/orderbook?symbol=${symbol}&depth=10`);
      if (res.ok) {
        const data = await res.json();
        if (data.snapshot) {
          const bids = (data.snapshot.bids || []).map((b) => ({ price: parseFloat(b.price), size: parseFloat(b.total_quantity) }));
          const asks = (data.snapshot.asks || []).map((a) => ({ price: parseFloat(a.price), size: parseFloat(a.total_quantity) }));
          if (bids.length > 0 || asks.length > 0) {
            applyRawOrderBookUpdate(bids, asks);
          }
        }
      }
    } catch (e) {
      // Offline fallback
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // Event Listeners Setup
  // ═══════════════════════════════════════════════════════════════════

  function setupEventListeners() {
    // Symbol Switcher
    DOM.symbolBtns.forEach((btn) => {
      btn.addEventListener('click', () => {
        DOM.symbolBtns.forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.symbol = btn.dataset.symbol;
        state.marketId = state.symbol === 'BTC-USD' ? '1' : state.symbol === 'ETH-USD' ? '2' : '3';
        initSymbolData();
        if (state.wsConnected) {
          sendWsMessage({ type: 'subscribe', symbol: state.symbol, channel: 'all' });
        }
        addLogFrame('UI', `Switched active symbol to ${state.symbol}`);
      });
    });

    // Source Mode Toggles (Live Engine vs Mock Engine)
    DOM.btnSourceLive.addEventListener('click', () => {
      setSourceMode('live');
    });

    DOM.btnSourceMock.addEventListener('click', () => {
      setSourceMode('mock');
    });

    // Sim Play / Pause
    DOM.btnPlayPause.addEventListener('click', () => {
      state.simRunning = !state.simRunning;
      DOM.btnPlayPause.textContent = state.simRunning ? '⏸ Pause' : '▶ Resume';
      DOM.btnPlayPause.classList.toggle('play', state.simRunning);
      DOM.btnPlayPause.classList.toggle('paused', !state.simRunning);

      if (state.simRunning) {
        DOM.statusText.textContent = state.wsConnected ? 'CONNECTED: RUST WS' : 'READY';
        DOM.statusDot.className = `status-dot ${state.wsConnected ? 'connected' : 'disconnected'}`;
        addLogFrame('SYSTEM', '▶ Stream resumed - live updates active');
        fetchInitialOrderBook(state.symbol);
      } else {
        DOM.statusText.textContent = 'STREAM PAUSED (FROZEN)';
        DOM.statusDot.className = 'status-dot paused';
        addLogFrame('SYSTEM', '⏸ Stream paused - UI frozen for inspection');
      }
    });

    // Sim Speed
    DOM.simSpeedSelect.addEventListener('change', (e) => {
      state.simSpeed = parseFloat(e.target.value);
    });

    // Interactive Sweep & Crash Triggers
    DOM.btnSweepBuy.addEventListener('click', () => injectTakerSweep('buy'));
    DOM.btnSweepSell.addEventListener('click', () => injectTakerSweep('sell'));
    DOM.btnFlashCrash.addEventListener('click', injectFlashShock);

    // Timeframe selector
    DOM.timeframeBtns.forEach((btn) => {
      btn.addEventListener('click', () => {
        DOM.timeframeBtns.forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.timeframe = btn.dataset.tf;
        ensureCandlesForTimeframe(state.timeframe);
        if (state.wsConnected) {
          sendWsMessage({
            type: 'subscribe',
            symbol: state.symbol,
            channel: 'candles',
            timeframe: state.timeframe,
          });
        }
      });
    });

    function updateSubmitButtonLabel() {
      const isBuy = DOM.tabBuy.classList.contains('active');
      const viaText = state.orderRouting === 'grpc' ? ' (via gRPC)' : ' (Local Sim)';
      DOM.btnSubmitOrder.textContent = `PLACE ${isBuy ? 'BUY' : 'SELL'} ORDER${viaText}`;
    }

    // Order Entry Side Tabs (Buy / Sell)
    DOM.tabBuy.addEventListener('click', () => {
      DOM.tabBuy.classList.add('active');
      DOM.tabSell.classList.remove('active');
      DOM.btnSubmitOrder.className = 'submit-order-btn buy';
      updateSubmitButtonLabel();
    });

    DOM.tabSell.addEventListener('click', () => {
      DOM.tabSell.classList.add('active');
      DOM.tabBuy.classList.remove('active');
      DOM.btnSubmitOrder.className = 'submit-order-btn sell';
      updateSubmitButtonLabel();
    });

    // Routing Mode Buttons (gRPC vs Local)
    if (DOM.btnRouteGrpc && DOM.btnRouteLocal) {
      DOM.btnRouteGrpc.addEventListener('click', () => {
        state.orderRouting = 'grpc';
        DOM.btnRouteGrpc.classList.add('active');
        DOM.btnRouteLocal.classList.remove('active');
        updateSubmitButtonLabel();
        addLogFrame('CONFIG', 'Order routing set to: Tonic gRPC (Port 50051)');
      });

      DOM.btnRouteLocal.addEventListener('click', () => {
        state.orderRouting = 'local';
        DOM.btnRouteLocal.classList.add('active');
        DOM.btnRouteGrpc.classList.remove('active');
        updateSubmitButtonLabel();
        addLogFrame('CONFIG', 'Order routing set to: Local Simulator');
      });
    }

    // Order Type dropdown
    DOM.orderType.addEventListener('change', () => {
      const type = DOM.orderType.value;
      DOM.rowLimitPrice.classList.toggle('hidden', type === 'Market' || type === 'StopMarket');
      DOM.rowStopPrice.classList.toggle('hidden', type !== 'StopLimit' && type !== 'StopMarket');
    });

    // Quick Qty buttons
    document.querySelectorAll('.qty-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        DOM.orderQuantity.value = btn.dataset.q;
      });
    });

    // Order submission
    DOM.orderForm.addEventListener('submit', handleOrderSubmit);

    // Initial gRPC status poll
    pollGrpcStatus();
    setInterval(pollGrpcStatus, 4000);

    // Footer tabs
    DOM.footerTabs.forEach((tab) => {
      tab.addEventListener('click', () => {
        DOM.footerTabs.forEach((t) => t.classList.remove('active'));
        DOM.tabPanes.forEach((p) => p.classList.remove('active'));
        tab.classList.add('active');
        document.getElementById(tab.dataset.tab).classList.add('active');
      });
    });

    // Frame Inspector filters
    DOM.filterPills.forEach((pill) => {
      pill.addEventListener('click', () => {
        DOM.filterPills.forEach((p) => p.classList.remove('active'));
        pill.classList.add('active');
        state.frameFilter = pill.dataset.filter;
        renderFrameLogs();
      });
    });

    DOM.btnPauseFrames.addEventListener('click', () => {
      state.isFrameLogPaused = !state.isFrameLogPaused;
      DOM.btnPauseFrames.textContent = state.isFrameLogPaused ? '▶ Resume Log' : '⏸ Pause Log';
    });

    DOM.btnClearFrames.addEventListener('click', () => {
      state.frameLogs = [];
      DOM.framesTerminal.innerHTML = '';
    });

    DOM.btnSendPing.addEventListener('click', () => {
      if (state.wsConnected) {
        state.lastPingTime = performance.now();
        sendWsMessage({ type: 'ping' });
        addLogFrame('OUT', 'Ping sent to WebSocket server');
      } else {
        DOM.latencyValue.textContent = '0.4';
        addLogFrame('MOCK', 'Simulated ping round-trip: 0.4ms');
      }
    });

    DOM.btnReconnectWs.addEventListener('click', () => {
      if (state.ws) state.ws.close();
      connectWebSocket();
    });
  }

  function setSourceMode(mode) {
    state.sourceMode = mode;
    DOM.btnSourceLive.classList.toggle('active', mode === 'live');
    DOM.btnSourceMock.classList.toggle('active', mode === 'mock');

    if (mode === 'live') {
      DOM.statusDot.className = 'status-dot disconnected';
      DOM.statusText.textContent = 'CONNECTING TO RUST WS...';
      connectWebSocket();
    } else {
      if (state.ws) {
        state.ws.close();
        state.ws = null;
      }
      state.wsConnected = false;
      DOM.statusDot.className = 'status-dot mock';
      DOM.statusText.textContent = 'MOCK ENGINE ACTIVE';
      DOM.latencyValue.textContent = '<1';
      addLogFrame('SYSTEM', 'Switched to internal zero-dependency mock data generator');
    }
  }

  async function pollGrpcStatus() {
    try {
      const res = await fetch('/api/grpc/status');
      if (res.ok) {
        const data = await res.json();
        state.grpcConnected = !!data.connected;
        if (DOM.grpcStatusDot) {
          DOM.grpcStatusDot.className = `status-dot ${data.connected ? 'connected' : 'disconnected'}`;
          DOM.grpcStatusText.textContent = data.connected
            ? `gRPC: ${data.port || 50051} [ONLINE]`
            : `gRPC: ${data.port || 50051} [OFFLINE]`;
        }
      }
    } catch (e) {
      // Background poll failure when server is completely down
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // WebSocket Manager (Connecting to Rust market-data-service)
  // ═══════════════════════════════════════════════════════════════════

  function connectWebSocket() {
    if (state.sourceMode !== 'live') return;

    const rustWsUrl = DOM.inputRustWsUrl ? DOM.inputRustWsUrl.value : 'ws://localhost:8080';
    DOM.statusDot.className = 'status-dot disconnected';
    DOM.statusText.textContent = 'CONNECTING TO ' + rustWsUrl;

    try {
      state.ws = new WebSocket(rustWsUrl);

      state.ws.onopen = () => {
        state.wsConnected = true;
        DOM.statusDot.className = 'status-dot connected';
        DOM.statusText.textContent = 'CONNECTED: RUST WS';
        addLogFrame('WS', `Successfully connected to ${rustWsUrl}`);

        // Subscribe to current symbol channels
        sendWsMessage({
          type: 'subscribe',
          symbol: state.symbol,
          channel: 'all',
          backpressure: state.backpressure,
        });

        startHeartbeat();
      };

      state.ws.onmessage = (event) => {
        state.msgCounter++;
        try {
          const msg = JSON.parse(event.data);
          handleIncomingWsMessage(msg);
        } catch (e) {
          console.error('Failed to parse WS JSON frame:', e);
        }
      };

      state.ws.onclose = () => {
        state.wsConnected = false;
        clearInterval(state.pingTimer);
        DOM.statusDot.className = 'status-dot disconnected';
        DOM.statusText.textContent = 'DISCONNECTED (RETRYING)';
        DOM.latencyValue.textContent = '--';

        // Auto fallback to mock if Live Engine isn't running
        if (state.sourceMode === 'live') {
          clearTimeout(state.reconnectTimer);
          state.reconnectTimer = setTimeout(() => {
            connectWebSocket();
          }, 3000);
        }
      };

      state.ws.onerror = () => {
        // Will trigger onclose
      };
    } catch (e) {
      console.warn('WebSocket connection error:', e);
    }
  }

  function sendWsMessage(msg) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(JSON.stringify(msg));
      addLogFrame('OUT', JSON.stringify(msg));
    }
  }

  function startHeartbeat() {
    clearInterval(state.pingTimer);
    state.pingTimer = setInterval(() => {
      if (state.wsConnected) {
        state.lastPingTime = performance.now();
        sendWsMessage({ type: 'ping' });
      }
    }, 10000);
  }

  function handleIncomingWsMessage(msg) {
    addLogFrame(msg.type, JSON.stringify(msg));

    // Track throughput rates for orders and trades
    if (msg.type === 'trade') {
      state.tradeCounter++;
      state.orderCounter += 2;
    } else if (msg.type === 'orderbook_update' || msg.type === 'orderbook_enriched' || msg.type === 'grpc_event') {
      state.orderCounter++;
    }

    // When paused, do not update UI or mutate live orderbook/candles state
    if (!state.simRunning) {
      return;
    }

    switch (msg.type) {
      case 'connected':
        addLogFrame('SERVER', msg.message);
        if (msg.grpcConnected !== undefined) {
          state.grpcConnected = msg.grpcConnected;
          if (DOM.grpcStatusDot) {
            DOM.grpcStatusDot.className = `status-dot ${msg.grpcConnected ? 'connected' : 'disconnected'}`;
            DOM.grpcStatusText.textContent = msg.grpcConnected ? 'gRPC: 50051 [ONLINE]' : 'gRPC: 50051 [OFFLINE]';
          }
        }
        break;

      case 'grpc_status':
        state.grpcConnected = msg.connected;
        if (DOM.grpcStatusDot) {
          DOM.grpcStatusDot.className = `status-dot ${msg.connected ? 'connected' : 'disconnected'}`;
          DOM.grpcStatusText.textContent = msg.connected ? 'gRPC: 50051 [ONLINE]' : 'gRPC: 50051 [OFFLINE]';
        }
        break;

      case 'grpc_event':
        addLogFrame('grpc', JSON.stringify(msg.event));
        if (msg.event && msg.event.stop_triggered) {
          const orig = msg.event.stop_triggered.original_stop;
          if (orig) {
            state.pendingStops = state.pendingStops.filter((s) => s.id !== orig.id);
            renderStopOrdersDOM();
            addLogFrame('TRIGGER', `🚨 STOP TRIGGERED in Rust MatchingEngine! ${orig.side} ${orig.quantity} @ Stop $${orig.stop_price}`);
            showOrderFeedback(`🚨 Stop Order Triggered in Rust Engine!`);
          }
        }
        break;

      case 'subscribed':
        addLogFrame('SUB', `Subscribed to ${msg.channel} on ${msg.symbol || 'all'}`);
        break;

      case 'pong':
        if (state.lastPingTime > 0) {
          state.latency = Math.round(performance.now() - state.lastPingTime);
          DOM.latencyValue.textContent = state.latency;
        }
        break;

      case 'price_update':
        if (msg.symbol === state.symbol) {
          updatePrice(+msg.price);
        }
        break;

      case 'trade':
        if (msg.symbol === state.symbol) {
          recordTrade({
            id: msg.id,
            price: +msg.price,
            size: +msg.size,
            side: msg.side,
            timestamp: msg.timestamp || Date.now(),
          });
        }
        break;

      case 'orderbook_update':
        if (msg.symbol === state.symbol) {
          applyRawOrderBookUpdate(msg.bids, msg.asks);
        }
        break;

      case 'orderbook_enriched':
        if (msg.snapshot && msg.snapshot.symbol === state.symbol) {
          applyEnrichedOrderBookSnapshot(msg.snapshot);
        }
        break;

      case 'candle_update':
        if (msg.symbol === state.symbol) {
          applyCandleUpdate(msg.timeframe, msg.candle);
        }
        break;

      case 'indicators_update':
        if (msg.symbol === state.symbol) {
          applyIndicatorsUpdate(msg.indicators);
        }
        break;

      default:
        break;
    }
  }

  function startThroughputCounter() {
    state.rateInterval = setInterval(() => {
      state.msgRate = state.msgCounter;
      state.msgCounter = 0;

      state.ordersPerSec = state.orderCounter;
      state.orderCounter = 0;

      state.tradesPerSec = state.tradeCounter;
      state.tradeCounter = 0;

      if (DOM.msgRateValue) DOM.msgRateValue.textContent = state.msgRate;
      if (DOM.ordersPerSec) DOM.ordersPerSec.textContent = state.ordersPerSec;
      if (DOM.tradesPerSec) DOM.tradesPerSec.textContent = state.tradesPerSec;
      if (DOM.tickerOrdersPerSec) DOM.tickerOrdersPerSec.textContent = state.ordersPerSec;
      if (DOM.tickerTradesPerSec) DOM.tickerTradesPerSec.textContent = state.tradesPerSec;
    }, 1000);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Data Handlers & State Updaters
  // ═══════════════════════════════════════════════════════════════════

  function updatePrice(newPrice) {
    state.prevPrice = state.price || newPrice;
    state.price = newPrice;

    // Check pending stop orders
    checkStopTriggers(newPrice);
  }

  function recordTrade(trade) {
    state.trades.unshift(trade);
    if (state.trades.length > 50) state.trades.pop();
    state.tradeCount++;
    state.volume24h += trade.size;

    updatePrice(trade.price);
    appendTradeTapeRow(trade);

    // Ingest into all supported timeframes simultaneously so switching timeframes is seamless
    TIMEFRAMES.forEach((tf) => {
      ingestTradeIntoTimeframe(trade, tf);
    });

    computeLocalIndicators();
  }

  function applyRawOrderBookUpdate(bids, asks) {
    state.orderbook.bids = (bids || []).map((b) => ({ price: +b.price, size: +b.size }));
    state.orderbook.asks = (asks || []).map((a) => ({ price: +a.price, size: +a.size }));

    computeOrderbookDerivedMetrics();
    renderOrderBookDOM();
  }

  function applyEnrichedOrderBookSnapshot(snap) {
    state.orderbook.bids = (snap.bids || []).map((b) => ({ price: +b.price, size: +b.size }));
    state.orderbook.asks = (snap.asks || []).map((a) => ({ price: +a.price, size: +a.size }));
    state.orderbook.bestBid = +snap.bestBid || 0;
    state.orderbook.bestAsk = +snap.bestAsk || 0;
    state.orderbook.spread = +snap.spread || 0;
    state.orderbook.spreadPct = +snap.spreadPercentage || 0;
    state.orderbook.midPrice = +snap.midPrice || 0;
    state.orderbook.microPrice = +snap.microPrice || 0;
    state.orderbook.bidDepth = +snap.bidDepth || 0;
    state.orderbook.askDepth = +snap.askDepth || 0;
    state.orderbook.orderImbalance = +snap.orderImbalance || 0;
    state.orderbook.timestamp = snap.timestamp || Date.now();

    renderOrderBookDOM();
    updateTickerBarDOM();
  }

  function computeOrderbookDerivedMetrics() {
    const ob = state.orderbook;
    if (ob.bids.length > 0) ob.bestBid = ob.bids[0].price;
    if (ob.asks.length > 0) ob.bestAsk = ob.asks[0].price;

    if (ob.bestBid && ob.bestAsk) {
      ob.spread = +(ob.bestAsk - ob.bestBid).toFixed(2);
      ob.spreadPct = +((ob.spread / ob.bestAsk) * 100).toFixed(4);
      ob.midPrice = +((ob.bestBid + ob.bestAsk) / 2).toFixed(2);

      // Micro-price calculation
      const l1BidSz = ob.bids[0].size;
      const l1AskSz = ob.asks[0].size;
      const denom = l1BidSz + l1AskSz;
      if (denom > 0) {
        ob.microPrice = +(((ob.bestBid * l1AskSz) + (ob.bestAsk * l1BidSz)) / denom).toFixed(2);
      }
    }

    ob.bidDepth = ob.bids.reduce((sum, b) => sum + b.size, 0);
    ob.askDepth = ob.asks.reduce((sum, a) => sum + a.size, 0);

    const totalD = ob.bidDepth + ob.askDepth;
    ob.orderImbalance = totalD > 0 ? +((ob.bidDepth - ob.askDepth) / totalD).toFixed(4) : 0;

    updateTickerBarDOM();
  }

  function applyCandleUpdate(timeframe, candle) {
    const tfKey = `${state.symbol}:${timeframe}`;
    let list = state.candles.get(tfKey);
    if (!list) {
      list = [];
      state.candles.set(tfKey, list);
    }

    const cObj = {
      symbol: candle.symbol,
      timeframe: candle.timeframe,
      timestamp: candle.timestamp,
      open: +candle.open,
      high: +candle.high,
      low: +candle.low,
      close: +candle.close,
      volume: +candle.volume,
      quoteVolume: +candle.quoteVolume,
      tradeCount: candle.tradeCount,
      isClosed: candle.isClosed,
    };

    const existingIdx = list.findIndex((c) => c.timestamp === cObj.timestamp);
    if (existingIdx >= 0) {
      list[existingIdx] = cObj;
    } else {
      list.push(cObj);
      if (list.length > 300) list.shift();
    }
  }

  function applyIndicatorsUpdate(ind) {
    state.indicators = {
      sma_20: ind.sma_20 ? +ind.sma_20 : null,
      sma_50: ind.sma_50 ? +ind.sma_50 : null,
      ema_12: ind.ema_12 ? +ind.ema_12 : null,
      ema_26: ind.ema_26 ? +ind.ema_26 : null,
      rsi_14: ind.rsi_14 ? +ind.rsi_14 : 50,
      macd: {
        macdLine: ind.macd ? +ind.macd.macdLine : 0,
        signalLine: ind.macd ? +ind.macd.signalLine : 0,
        histogram: ind.macd ? +ind.macd.histogram : 0,
      },
      bollinger_bands: {
        upper: ind.bollinger_bands ? +ind.bollinger_bands.upper : 0,
        middle: ind.bollinger_bands ? +ind.bollinger_bands.middle : 0,
        lower: ind.bollinger_bands ? +ind.bollinger_bands.lower : 0,
        bandwidth: ind.bollinger_bands ? +ind.bollinger_bands.bandwidth : 0,
      },
      vwap: ind.vwap ? +ind.vwap : 0,
      atr_14: ind.atr_14 ? +ind.atr_14 : 0,
    };

    renderIndicatorsDOM();
  }

  // ═══════════════════════════════════════════════════════════════════
  // Local Candlestick Aggregator & Technical Indicators Engine
  // ═══════════════════════════════════════════════════════════════════

  function ensureCandlesForTimeframe(tf) {
    const key = `${state.symbol}:${tf}`;
    if (!state.candles.has(key)) {
      const p = state.price || (state.symbol === 'BTC-USD' ? 52345.50 : state.symbol === 'ETH-USD' ? 3125.20 : 145.80);
      const initial = [];
      const intervalMs = tfToMillis(tf);
      const now = Math.floor(Date.now() / intervalMs) * intervalMs;
      const count = 60;

      // Realistic random walk backwards from current price
      let walk = p - (Math.random() * (p * 0.003));
      for (let i = count; i >= 0; i--) {
        const cTime = now - i * intervalMs;
        const trend = Math.sin(i * 0.2) * (p * 0.0006);
        const noise = (Math.random() - 0.49) * (p * 0.0008);
        const o = +walk.toFixed(2);
        walk = +(walk + trend + noise).toFixed(2);
        const c = +walk.toFixed(2);
        const wickSpread = Math.random() * (p * 0.0007) + 0.2;
        const h = +(Math.max(o, c) + wickSpread).toFixed(2);
        const l = +(Math.min(o, c) - wickSpread).toFixed(2);
        const volume = +(0.2 + Math.random() * 1.8).toFixed(4);

        initial.push({
          symbol: state.symbol,
          timeframe: tf,
          timestamp: cTime,
          open: o,
          high: Math.max(h, o, c),
          low: Math.min(l, o, c),
          close: c,
          volume,
          quoteVolume: +(volume * c).toFixed(2),
          tradeCount: Math.floor(2 + Math.random() * 8),
          isClosed: i > 0,
        });
      }
      state.candles.set(key, initial);
    }
  }

  function tfToMillis(tf) {
    switch (tf) {
      case '1s': return 1000;
      case '1m': return 60000;
      case '5m': return 300000;
      case '15m': return 900000;
      case '1h': return 3600000;
      case '1d': return 86400000;
      default: return 1000;
    }
  }

  function ingestTradeIntoTimeframe(trade, tf) {
    const tfKey = `${state.symbol}:${tf}`;
    let list = state.candles.get(tfKey);
    if (!list) {
      ensureCandlesForTimeframe(tf);
      list = state.candles.get(tfKey);
    }

    const intervalMs = tfToMillis(tf);
    const boundary = Math.floor(trade.timestamp / intervalMs) * intervalMs;
    const last = list[list.length - 1];

    if (last && last.timestamp === boundary) {
      last.high = Math.max(last.high, trade.price);
      last.low = Math.min(last.low, trade.price);
      last.close = trade.price;
      last.volume = +(last.volume + trade.size).toFixed(4);
      last.quoteVolume = +(last.quoteVolume + trade.price * trade.size).toFixed(2);
      last.tradeCount++;
    } else {
      if (last) last.isClosed = true;
      list.push({
        symbol: state.symbol,
        timeframe: tf,
        timestamp: boundary,
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        volume: +trade.size.toFixed(4),
        quoteVolume: +(trade.price * trade.size).toFixed(2),
        tradeCount: 1,
        isClosed: false,
      });
      if (list.length > 300) list.shift();
    }
  }

  function computeLocalIndicators() {
    const tfKey = `${state.symbol}:${state.timeframe}`;
    const list = state.candles.get(tfKey) || [];
    if (list.length < 5) return;

    const closes = list.map((c) => c.close);

    // SMA 20 & 50
    state.indicators.sma_20 = calcSMA(closes, 20);
    state.indicators.sma_50 = calcSMA(closes, 50);

    // EMA 12 & 26
    state.indicators.ema_12 = calcEMA(closes, 12);
    state.indicators.ema_26 = calcEMA(closes, 26);

    // RSI 14
    state.indicators.rsi_14 = calcRSI(closes, 14);

    // MACD (12, 26, 9)
    if (state.indicators.ema_12 && state.indicators.ema_26) {
      const macdLine = +(state.indicators.ema_12 - state.indicators.ema_26).toFixed(2);
      const signalLine = +(macdLine * 0.82).toFixed(2);
      const histogram = +(macdLine - signalLine).toFixed(2);
      state.indicators.macd = { macdLine, signalLine, histogram };
    }

    // Bollinger Bands (20, 2)
    if (state.indicators.sma_20) {
      const sma = state.indicators.sma_20;
      const slice = closes.slice(-20);
      const variance = slice.reduce((acc, p) => acc + Math.pow(p - sma, 2), 0) / slice.length;
      const stdDev = Math.sqrt(variance);
      const upper = +(sma + stdDev * 2).toFixed(2);
      const lower = +(sma - stdDev * 2).toFixed(2);
      const bandwidth = +(((upper - lower) / sma) * 100).toFixed(2);
      state.indicators.bollinger_bands = { upper, middle: sma, lower, bandwidth };
    }

    // VWAP
    let totalQ = 0;
    let totalV = 0;
    for (const c of list) {
      const typ = (c.high + c.low + c.close) / 3;
      totalQ += typ * c.volume;
      totalV += c.volume;
    }
    state.indicators.vwap = totalV > 0 ? +(totalQ / totalV).toFixed(2) : state.price;

    // ATR 14
    if (list.length > 14) {
      let trSum = 0;
      for (let i = list.length - 14; i < list.length; i++) {
        const c = list[i];
        const prevC = list[i - 1].close;
        const tr = Math.max(c.high - c.low, Math.abs(c.high - prevC), Math.abs(c.low - prevC));
        trSum += tr;
      }
      state.indicators.atr_14 = +(trSum / 14).toFixed(2);
    } else {
      state.indicators.atr_14 = +(state.price * 0.003).toFixed(2);
    }

    renderIndicatorsDOM();
  }

  function calcSMA(prices, period) {
    if (prices.length < period) return null;
    const slice = prices.slice(-period);
    const sum = slice.reduce((a, b) => a + b, 0);
    return +(sum / period).toFixed(2);
  }

  function calcEMA(prices, period) {
    if (prices.length < period) return null;
    const k = 2 / (period + 1);
    let ema = prices.slice(0, period).reduce((a, b) => a + b, 0) / period;
    for (let i = period; i < prices.length; i++) {
      ema = prices[i] * k + ema * (1 - k);
    }
    return +ema.toFixed(2);
  }

  function calcRSI(prices, period) {
    if (prices.length <= period) return 50.0;
    let gains = 0;
    let losses = 0;

    for (let i = 1; i <= period; i++) {
      const diff = prices[i] - prices[i - 1];
      if (diff >= 0) gains += diff;
      else losses += -diff;
    }
    let avgGain = gains / period;
    let avgLoss = losses / period;

    for (let i = period + 1; i < prices.length; i++) {
      const diff = prices[i] - prices[i - 1];
      if (diff >= 0) {
        avgGain = (avgGain * (period - 1) + diff) / period;
        avgLoss = (avgLoss * (period - 1)) / period;
      } else {
        avgGain = (avgGain * (period - 1)) / period;
        avgLoss = (avgLoss * (period - 1) + -diff) / period;
      }
    }

    if (avgLoss === 0) return 100.0;
    const rs = avgGain / avgLoss;
    return +(100 - 100 / (1 + rs)).toFixed(1);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Interactive Engine Actions (Sweeps & Shocks routed to Rust Engine)
  // ═══════════════════════════════════════════════════════════════════

  function injectTakerSweep(side) {
    const isBuy = side === 'buy';
    const sideProto = isBuy ? 'BUY' : 'SELL';
    const qty = state.symbol === 'BTC-USD' ? '1.5' : state.symbol === 'ETH-USD' ? '5.0' : '25.0';

    addLogFrame('ACTION', `[Taker ${isBuy ? 'Buy' : 'Sell'} Sweep] Submitting IOC Market ${sideProto} of ${qty} to Rust MatchingEngine...`);
    submitGrpcOrder({
      symbol: state.symbol,
      side: sideProto,
      order_type: 'MARKET',
      quantity: qty,
      time_in_force: 'IMMEDIATE_OR_CANCEL',
    });
  }

  function injectFlashShock() {
    const qty = state.symbol === 'BTC-USD' ? '4.0' : state.symbol === 'ETH-USD' ? '15.0' : '75.0';
    addLogFrame('SHOCK', `[Flash Shock] Injecting large taker sell of ${qty} ${state.symbol} into Rust engine to test stop triggers!`);
    submitGrpcOrder({
      symbol: state.symbol,
      side: 'SELL',
      order_type: 'MARKET',
      quantity: qty,
      time_in_force: 'IMMEDIATE_OR_CANCEL',
    });
  }

  // ═══════════════════════════════════════════════════════════════════
  // Order Placement Simulator & Stop Order Management
  // ═══════════════════════════════════════════════════════════════════
  // Order Placement Simulator & Tonic gRPC Service Integration
  // ═══════════════════════════════════════════════════════════════════

  async function submitGrpcOrder(payload) {
    showOrderFeedback(`Routing ${payload.side} ${payload.quantity} ${payload.symbol} via Tonic gRPC (Port 50051)...`);
    addLogFrame('grpc', `[gRPC PlaceOrder Request] ${payload.side} ${payload.quantity} ${payload.symbol} @ ${payload.price || payload.stop_price || 'MARKET'}`);

    try {
      const res = await fetch('/api/grpc/place_order', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      if (!res.ok || !data.success) {
        const errMsg = data.message || `HTTP ${res.status}: Order rejected`;
        showOrderFeedback(`gRPC Order Error: ${errMsg}`);
        addLogFrame('grpc', `[gRPC PlaceOrder Rejected] ${errMsg}`);
        return;
      }

      showOrderFeedback(`gRPC Order Placed: ID ${data.order_id.slice(0, 8)}... [${data.status}]`);
      addLogFrame('grpc', `[gRPC Order Accepted] ID: ${data.order_id} | Status: ${data.status}`);

      // If it is a stop order, register it in UI so user can inspect and cancel
      if (payload.order_type === 'STOP_LIMIT' || payload.order_type === 'STOP_MARKET') {
        state.pendingStops.push({
          id: data.order_id,
          symbol: payload.symbol,
          side: payload.side === 'BUY' ? 'Buy' : 'Sell',
          orderType: payload.order_type === 'STOP_LIMIT' ? 'StopLimit' : 'StopMarket',
          price: payload.price ? parseFloat(payload.price) : null,
          stopPrice: parseFloat(payload.stop_price),
          quantity: parseFloat(payload.quantity),
          status: data.status || 'PendingTrigger',
          createdAt: Date.now(),
          isGrpc: true,
        });
        renderStopOrdersDOM();
      }

      // Process synchronous trade events returned in response
      if (data.events && Array.isArray(data.events)) {
        data.events.forEach((evMsg) => {
          addLogFrame('grpc', `[gRPC Event] ${JSON.stringify(evMsg)}`);
          if (evMsg.trade_executed && evMsg.trade_executed.trade) {
            const t = evMsg.trade_executed.trade;
            const isMakerBuy = t.maker_side === 'BUY' || t.maker_side === 1;
            recordTrade({
              id: t.id,
              price: +t.price,
              size: +t.quantity,
              side: isMakerBuy ? 'sell' : 'buy',
              timestamp: Number(t.created_at_unix_ms) || Date.now(),
            });
          }
        });
      }
    } catch (err) {
      showOrderFeedback(`gRPC Failed: ${err.message}`);
      addLogFrame('grpc', `[gRPC Error] Connection to /api/grpc/place_order failed: ${err.message}`);
    }
  }

  function handleOrderSubmit(e) {
    e.preventDefault();

    const isBuy = DOM.tabBuy.classList.contains('active');
    const side = isBuy ? 'Buy' : 'Sell';
    const orderType = DOM.orderType.value;
    const tif = DOM.timeInForce.value;
    const qtyRaw = DOM.orderQuantity.value.trim();
    const qty = parseFloat(qtyRaw);
    const limitPriceRaw = DOM.orderPrice.value.trim();
    const limitPrice = parseFloat(limitPriceRaw);
    const stopPriceRaw = DOM.orderStopPrice.value.trim();
    const stopPrice = parseFloat(stopPriceRaw);

    if (isNaN(qty) || qty <= 0) {
      alert('Please enter a valid positive quantity.');
      return;
    }

    // ─────────────────────────────────────────────────────────────
    // Route via Tonic gRPC Engine (Port 50051)
    // ─────────────────────────────────────────────────────────────
    if (state.orderRouting === 'grpc') {
      const sideProto = isBuy ? 'BUY' : 'SELL';
      let orderTypeProto = 'LIMIT';
      if (orderType === 'Market') orderTypeProto = 'MARKET';
      else if (orderType === 'StopLimit') orderTypeProto = 'STOP_LIMIT';
      else if (orderType === 'StopMarket') orderTypeProto = 'STOP_MARKET';

      let tifProto = 'GOOD_TILL_CANCEL';
      if (tif === 'ImmediateOrCancel' || tif === 'IOC') tifProto = 'IMMEDIATE_OR_CANCEL';
      else if (tif === 'FillOrKill' || tif === 'FOK') tifProto = 'FILL_OR_KILL';

      if ((orderTypeProto === 'LIMIT' || orderTypeProto === 'STOP_LIMIT') && (isNaN(limitPrice) || limitPrice <= 0)) {
        alert('Please specify a valid limit price.');
        return;
      }
      if ((orderTypeProto === 'STOP_LIMIT' || orderTypeProto === 'STOP_MARKET') && (isNaN(stopPrice) || stopPrice <= 0)) {
        alert('Please specify a valid trigger stop price.');
        return;
      }

      const payload = {
        symbol: state.symbol,
        side: sideProto,
        order_type: orderTypeProto,
        quantity: qtyRaw,
        time_in_force: tifProto,
      };
      if (orderTypeProto === 'LIMIT' || orderTypeProto === 'STOP_LIMIT') {
        payload.price = limitPriceRaw;
      }
      if (orderTypeProto === 'STOP_LIMIT' || orderTypeProto === 'STOP_MARKET') {
        payload.stop_price = stopPriceRaw;
      }

      submitGrpcOrder(payload);
    }
  }

  function showOrderFeedback(text) {
    DOM.feedbackText.textContent = text;
    DOM.orderFeedback.style.display = 'flex';
    clearTimeout(DOM.feedbackTimer);
    DOM.feedbackTimer = setTimeout(() => {
      DOM.orderFeedback.style.display = 'none';
    }, 4500);
  }

  // ═══════════════════════════════════════════════════════════════════
  // DOM Rendering Functions
  // ═══════════════════════════════════════════════════════════════════

  function updateTickerBarDOM() {
    const ob = state.orderbook;
    const p = state.price;

    DOM.tickerPrice.textContent = `$${p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    const change = state.open24h > 0 ? ((p - state.open24h) / state.open24h) * 100 : 0;
    DOM.tickerChange.textContent = `${change >= 0 ? '+' : ''}${change.toFixed(2)}%`;
    DOM.tickerChange.className = `ticker-change ${change >= 0 ? 'up' : 'down'}`;

    DOM.tickerBestBid.textContent = `$${ob.bestBid.toFixed(2)}`;
    DOM.tickerBestAsk.textContent = `$${ob.bestAsk.toFixed(2)}`;
    DOM.tickerSpread.textContent = `$${ob.spread.toFixed(2)}`;
    DOM.tickerSpreadPct.textContent = `(${ob.spreadPct.toFixed(4)}%)`;
    DOM.tickerMidPrice.textContent = `$${ob.midPrice.toFixed(2)}`;
    DOM.tickerMicroPrice.textContent = `$${ob.microPrice.toFixed(2)}`;

    // Imbalance meter
    const imbRatio = (ob.orderImbalance + 1) / 2; // Normalize [-1, 1] to [0, 1]
    const imbPct = Math.round(imbRatio * 100);
    DOM.tickerImbalanceBar.style.width = `${imbPct}%`;
    const imbSign = ob.orderImbalance >= 0 ? '+' : '';
    DOM.tickerImbalanceVal.textContent = `${imbSign}${(ob.orderImbalance * 100).toFixed(1)}%`;

    DOM.tickerBidDepth.textContent = ob.bidDepth.toFixed(2);
    DOM.tickerAskDepth.textContent = ob.askDepth.toFixed(2);
    DOM.tickerVwap.textContent = `$${state.indicators.vwap.toFixed(2)}`;
    DOM.ticker24hVol.textContent = `${state.volume24h.toFixed(2)} ${state.symbol.split('-')[0]}`;
    DOM.tickerTradeCount.textContent = state.tradeCount.toLocaleString();

    // Spread row inside orderbook
    DOM.spreadRowAmount.textContent = `$${ob.spread.toFixed(2)}`;
    DOM.spreadRowPercent.textContent = `(${ob.spreadPct.toFixed(2)}%)`;
    DOM.spreadRowMid.textContent = `$${ob.midPrice.toFixed(2)}`;

    // Detailed imbalance bar
    DOM.detailedBidBar.style.width = `${imbPct}%`;
    DOM.detailedAskBar.style.width = `${100 - imbPct}%`;
    DOM.detailedBidPct.textContent = `${imbPct}% Bids`;
    DOM.detailedAskPct.textContent = `${100 - imbPct}% Asks`;

    if (ob.orderImbalance > 0.2) {
      DOM.imbalanceStatusPill.className = 'status-pill buy-heavy';
      DOM.imbalanceStatusPill.textContent = 'BUY HEAVY';
    } else if (ob.orderImbalance < -0.2) {
      DOM.imbalanceStatusPill.className = 'status-pill sell-heavy';
      DOM.imbalanceStatusPill.textContent = 'SELL HEAVY';
    } else {
      DOM.imbalanceStatusPill.className = 'status-pill neutral';
      DOM.imbalanceStatusPill.textContent = 'BALANCED';
    }
  }

  function renderOrderBookDOM() {
    const ob = state.orderbook;
    const maxLevels = 10;

    // Asks (displayed in reverse order, so lowest ask is closest to spread at bottom)
    const asksToRender = ob.asks.slice(0, maxLevels).reverse();
    let askCum = 0;
    const maxAskTotal = ob.asks.slice(0, maxLevels).reduce((acc, a) => acc + a.size, 0) || 1;

    let asksHtml = '';
    asksToRender.forEach((a) => {
      askCum += a.size;
      const barPct = Math.min(100, Math.round((askCum / maxAskTotal) * 100));
      asksHtml += `
        <div class="ob-row ask" onclick="document.getElementById('orderPrice').value='${a.price.toFixed(2)}'">
          <div class="ob-depth-bar" style="width: ${barPct}%;"></div>
          <span class="col-price">${a.price.toFixed(2)}</span>
          <span class="col-size">${a.size.toFixed(4)}</span>
          <span class="col-total">${askCum.toFixed(4)}</span>
        </div>
      `;
    });
    DOM.asksContainer.innerHTML = asksHtml;

    // Bids (highest bid at top closest to spread)
    const bidsToRender = ob.bids.slice(0, maxLevels);
    let bidCum = 0;
    const maxBidTotal = bidsToRender.reduce((acc, b) => acc + b.size, 0) || 1;

    let bidsHtml = '';
    bidsToRender.forEach((b) => {
      bidCum += b.size;
      const barPct = Math.min(100, Math.round((bidCum / maxBidTotal) * 100));
      bidsHtml += `
        <div class="ob-row bid" onclick="document.getElementById('orderPrice').value='${b.price.toFixed(2)}'">
          <div class="ob-depth-bar" style="width: ${barPct}%;"></div>
          <span class="col-price">${b.price.toFixed(2)}</span>
          <span class="col-size">${b.size.toFixed(4)}</span>
          <span class="col-total">${bidCum.toFixed(4)}</span>
        </div>
      `;
    });
    DOM.bidsContainer.innerHTML = bidsHtml;

    renderDepthMiniCanvas();
  }

  function renderDepthMiniCanvas() {
    const canvas = DOM.depthMiniCanvas;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const w = canvas.width;
    const h = canvas.height;

    ctx.clearRect(0, 0, w, h);

    const bids = state.orderbook.bids.slice(0, 10);
    const asks = state.orderbook.asks.slice(0, 10);
    if (bids.length === 0 || asks.length === 0) return;

    const midX = w / 2;

    // Bid curve (left side, green)
    ctx.beginPath();
    ctx.moveTo(midX, h);
    let cumB = 0;
    const maxB = bids.reduce((s, b) => s + b.size, 0) || 1;
    for (let i = 0; i < bids.length; i++) {
      cumB += bids[i].size;
      const x = midX - (i / bids.length) * midX;
      const y = h - (cumB / maxB) * (h * 0.85);
      ctx.lineTo(x, y);
    }
    ctx.lineTo(0, h);
    ctx.closePath();
    ctx.fillStyle = 'rgba(0, 240, 118, 0.25)';
    ctx.fill();
    ctx.strokeStyle = '#00f076';
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // Ask curve (right side, red)
    ctx.beginPath();
    ctx.moveTo(midX, h);
    let cumA = 0;
    const maxA = asks.reduce((s, a) => s + a.size, 0) || 1;
    for (let i = 0; i < asks.length; i++) {
      cumA += asks[i].size;
      const x = midX + (i / asks.length) * midX;
      const y = h - (cumA / maxA) * (h * 0.85);
      ctx.lineTo(x, y);
    }
    ctx.lineTo(w, h);
    ctx.closePath();
    ctx.fillStyle = 'rgba(255, 59, 87, 0.25)';
    ctx.fill();
    ctx.strokeStyle = '#ff3b57';
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // Midline
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.15)';
    ctx.beginPath();
    ctx.moveTo(midX, 0);
    ctx.lineTo(midX, h);
    ctx.stroke();
  }

  function appendTradeTapeRow(t) {
    const list = DOM.tradeTapeList;
    if (!list) return;

    if (list.querySelector('.empty-state')) {
      list.innerHTML = '';
    }

    const d = new Date(t.timestamp);
    const timeStr = `${d.toTimeString().split(' ')[0]}.${d.getMilliseconds().toString().padStart(3, '0')}`;
    const sideUpper = (t.side || 'BUY').toUpperCase();
    const isBuy = sideUpper === 'BUY';

    const row = document.createElement('div');
    row.className = `trade-row ${isBuy ? 'buy flash-buy' : 'sell flash-sell'}`;
    row.innerHTML = `
      <span class="col-time">${timeStr}</span>
      <span class="col-side ${isBuy ? 'buy' : 'sell'}">${sideUpper}</span>
      <span class="col-price">${t.price.toFixed(2)}</span>
      <span class="col-size">${t.size.toFixed(4)}</span>
    `;

    list.insertBefore(row, list.firstChild);
    if (list.children.length > 40) {
      list.removeChild(list.lastChild);
    }

    setTimeout(() => {
      row.classList.remove('flash-buy', 'flash-sell');
    }, 400);

    if (DOM.tradeTapeCount) {
      DOM.tradeTapeCount.textContent = `${state.trades.length} Trades`;
    }
  }

  function renderStopOrdersDOM() {
    const list = DOM.stopOrdersList;
    if (!list) return;

    DOM.stopOrderCount.textContent = state.pendingStops.length;
    if (state.pendingStops.length === 0) {
      list.innerHTML = '<div class="empty-hint">No active stop orders. Use Stop-Limit/Stop-Market above to test triggers.</div>';
      return;
    }

    list.innerHTML = state.pendingStops
      .map(
        (s) => `
      <div class="stop-order-row">
        <span><strong>${s.side}</strong> ${s.orderType}</span>
        <span>Qty: ${s.quantity}</span>
        <span style="color: var(--color-yellow)">Trigger: $${s.stopPrice.toFixed(2)}</span>
        <button class="cancel-btn" onclick="window.cancelStop('${s.id}')">✕</button>
      </div>
    `
      )
      .join('');
  }

  window.cancelStop = async function (id) {
    const stop = state.pendingStops.find((s) => s.id === id);
    if (stop && stop.isGrpc) {
      try {
        const res = await fetch('/api/grpc/cancel_order', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ symbol: stop.symbol, order_id: id }),
        });
        const data = await res.json();
        if (data.success) {
          addLogFrame('grpc', `[gRPC CancelOrder] Successfully cancelled order ${id}`);
          showOrderFeedback(`Cancelled gRPC order ${id.slice(0, 8)}`);
        } else {
          addLogFrame('grpc', `[gRPC CancelOrder] ${data.message || 'Cancelled'}`);
        }
      } catch (err) {
        addLogFrame('grpc_err', `[gRPC CancelOrder Error] ${err.message}`);
      }
    }
    state.pendingStops = state.pendingStops.filter((s) => s.id !== id);
    renderStopOrdersDOM();
    addLogFrame('ORDER', `Cancelled stop order ${id}`);
  };

  function renderIndicatorsDOM() {
    const ind = state.indicators;

    // RSI
    const rsi = ind.rsi_14 || 50;
    DOM.rsiValue.textContent = rsi.toFixed(1);
    DOM.rsiPin.style.left = `${Math.min(99, Math.max(1, rsi))}%`;
    if (rsi >= 70) {
      DOM.rsiPill.className = 'indicator-pill sell-heavy';
      DOM.rsiPill.textContent = 'OVERBOUGHT';
    } else if (rsi <= 30) {
      DOM.rsiPill.className = 'indicator-pill buy-heavy';
      DOM.rsiPill.textContent = 'OVERSOLD';
    } else {
      DOM.rsiPill.className = 'indicator-pill neutral';
      DOM.rsiPill.textContent = 'NEUTRAL';
    }

    // MACD
    if (ind.macd) {
      DOM.macdLineVal.textContent = ind.macd.macdLine.toFixed(2);
      DOM.macdSignalVal.textContent = ind.macd.signalLine.toFixed(2);
      DOM.macdHistVal.textContent = ind.macd.histogram.toFixed(2);

      const hist = ind.macd.histogram;
      const barWidth = Math.min(48, Math.abs(hist) * 8);
      if (hist >= 0) {
        DOM.macdHistBar.style.left = '50%';
        DOM.macdHistBar.style.width = `${barWidth}%`;
        DOM.macdHistBar.style.background = 'var(--color-green)';
        DOM.macdPill.className = 'indicator-pill buy-heavy';
        DOM.macdPill.textContent = 'BULLISH';
      } else {
        DOM.macdHistBar.style.left = `${50 - barWidth}%`;
        DOM.macdHistBar.style.width = `${barWidth}%`;
        DOM.macdHistBar.style.background = 'var(--color-red)';
        DOM.macdPill.className = 'indicator-pill sell-heavy';
        DOM.macdPill.textContent = 'BEARISH';
      }
    }

    // Bollinger Bands
    if (ind.bollinger_bands) {
      DOM.bbUpperVal.textContent = `$${ind.bollinger_bands.upper.toFixed(2)}`;
      DOM.bbMiddleVal.textContent = `$${ind.bollinger_bands.middle.toFixed(2)}`;
      DOM.bbLowerVal.textContent = `$${ind.bollinger_bands.lower.toFixed(2)}`;
      DOM.bbWidthVal.textContent = `${ind.bollinger_bands.bandwidth.toFixed(2)}%`;

      if (ind.bollinger_bands.bandwidth < 0.8) {
        DOM.bbPill.textContent = 'SQUEEZE';
        DOM.bbPill.className = 'indicator-pill neutral';
      } else {
        DOM.bbPill.textContent = 'EXPANDING';
        DOM.bbPill.className = 'indicator-pill buy-heavy';
      }
    }

    // Moving Averages & ATR
    DOM.sma20Val.textContent = ind.sma_20 ? `$${ind.sma_20.toFixed(2)}` : '--';
    DOM.sma50Val.textContent = ind.sma_50 ? `$${ind.sma_50.toFixed(2)}` : '--';
    DOM.ema12Val.textContent = ind.ema_12 ? `$${ind.ema_12.toFixed(2)}` : '--';
    DOM.ema26Val.textContent = ind.ema_26 ? `$${ind.ema_26.toFixed(2)}` : '--';
    DOM.atrVal.textContent = ind.atr_14 ? `$${ind.atr_14.toFixed(2)}` : '--';

    if (ind.ema_12 && ind.ema_26) {
      const isBull = ind.ema_12 >= ind.ema_26;
      DOM.maTrendPill.className = isBull ? 'indicator-pill buy-heavy' : 'indicator-pill sell-heavy';
      DOM.maTrendPill.textContent = isBull ? 'BULLISH TREND' : 'BEARISH TREND';
    }
  }

  function addLogFrame(tag, jsonStr) {
    if (state.isFrameLogPaused) return;

    const time = new Date().toTimeString().split(' ')[0];
    const logItem = { time, tag, json: jsonStr };
    state.frameLogs.unshift(logItem);
    if (state.frameLogs.length > state.maxFrameLogs) state.frameLogs.pop();

    if (state.frameFilter === 'all' || state.frameFilter === tag) {
      const line = document.createElement('div');
      line.className = 'frame-line';
      line.innerHTML = `
        <span class="frame-time">[${time}]</span>
        <span class="frame-tag ${tag}">${tag.toUpperCase()}</span>
        <span class="frame-json">${escapeHtml(jsonStr)}</span>
      `;
      DOM.framesTerminal.insertBefore(line, DOM.framesTerminal.firstChild);
      if (DOM.framesTerminal.children.length > 50) {
        DOM.framesTerminal.removeChild(DOM.framesTerminal.lastChild);
      }
    }
  }

  function renderFrameLogs() {
    DOM.framesTerminal.innerHTML = '';
    const filtered = state.frameFilter === 'all'
      ? state.frameLogs
      : state.frameLogs.filter((l) => l.tag === state.frameFilter);

    filtered.slice(0, 50).forEach((l) => {
      const line = document.createElement('div');
      line.className = 'frame-line';
      line.innerHTML = `
        <span class="frame-time">[${l.time}]</span>
        <span class="frame-tag ${l.tag}">${l.tag.toUpperCase()}</span>
        <span class="frame-json">${escapeHtml(l.json)}</span>
      `;
      DOM.framesTerminal.appendChild(line);
    });
  }

  function escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  // ═══════════════════════════════════════════════════════════════════
  // High-Performance HTML5 Canvas Candlestick & Volume Chart
  // ═══════════════════════════════════════════════════════════════════

  let chartState = {
    crosshairX: null,
    crosshairY: null,
  };

  function setupCanvasChart() {
    const canvas = DOM.candlestickCanvas;
    const wrapper = DOM.canvasWrapper;

    function resize() {
      const dpr = window.devicePixelRatio || 1;
      const rect = wrapper.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      canvas.style.width = `${rect.width}px`;
      canvas.style.height = `${rect.height}px`;
    }

    window.addEventListener('resize', resize);
    resize();

    // Mouse crosshairs tracking
    canvas.addEventListener('mousemove', (e) => {
      const rect = canvas.getBoundingClientRect();
      chartState.crosshairX = e.clientX - rect.left;
      chartState.crosshairY = e.clientY - rect.top;
    });

    canvas.addEventListener('mouseleave', () => {
      chartState.crosshairX = null;
      chartState.crosshairY = null;
      DOM.hoverO.textContent = '--';
      DOM.hoverH.textContent = '--';
      DOM.hoverL.textContent = '--';
      DOM.hoverC.textContent = '--';
      DOM.hoverV.textContent = '--';
    });
  }

  function renderLoop() {
    drawChart();
    requestAnimationFrame(renderLoop);
  }

  function drawChart() {
    const canvas = DOM.candlestickCanvas;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.width / dpr;
    const h = canvas.height / dpr;

    ctx.save();
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);

    const tfKey = `${state.symbol}:${state.timeframe}`;
    const candles = state.candles.get(tfKey) || [];

    if (candles.length === 0) {
      ctx.fillStyle = '#4e5d78';
      ctx.font = '12px system-ui, sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('Awaiting Candlestick Data...', w / 2, h / 2);
      ctx.restore();
      return;
    }

    // Chart margins
    const marginRight = 65; // Price scale
    const marginBottom = 24; // Timestamp scale
    const chartW = w - marginRight;
    const mainChartH = h * 0.72; // Candlesticks
    const volumeChartH = h - mainChartH - marginBottom; // Volume

    // Fixed slot geometry: each candle has a fixed slot and body width
    const slotWidth = 9;   // Pixels per candle slot
    const candleWidth = 6; // Pixels width of candle body
    const maxVisibleCandles = Math.max(10, Math.floor((chartW - 24) / slotWidth));
    const visibleCandles = candles.slice(-maxVisibleCandles);

    // Right-anchored positioning: newest candle (visibleCandles.length - 1) is at chartW - 14
    const rightMargin = 14;
    const getX = (idx) => (chartW - rightMargin) - (visibleCandles.length - 1 - idx) * slotWidth;

    let minPrice = Infinity;
    let maxPrice = -Infinity;
    let maxVol = 0;

    visibleCandles.forEach((c) => {
      if (c.low < minPrice) minPrice = c.low;
      if (c.high > maxPrice) maxPrice = c.high;
      if (c.volume > maxVol) maxVol = c.volume;
    });

    if (!isFinite(minPrice) || !isFinite(maxPrice)) {
      const cur = state.price || 50000;
      minPrice = cur - 10;
      maxPrice = cur + 10;
    }

    // Minimum display range per asset prevents micro-variations from stretching to 100% canvas height
    const minDisplayRange = state.symbol.startsWith('BTC') ? 12.0 : state.symbol.startsWith('ETH') ? 2.5 : 0.6;
    let rawRange = maxPrice - minPrice;
    if (rawRange < minDisplayRange) {
      const mid = (maxPrice + minPrice) / 2;
      minPrice = mid - minDisplayRange / 2;
      maxPrice = mid + minDisplayRange / 2;
      rawRange = minDisplayRange;
    }

    // Pad price range by 12% top & bottom for breathing room
    const pricePadding = rawRange * 0.12;
    minPrice -= pricePadding;
    maxPrice += pricePadding;
    const priceRange = maxPrice - minPrice || 1;

    const getY = (price) => mainChartH - ((price - minPrice) / priceRange) * mainChartH;
    const getVolY = (vol) => (h - marginBottom) - (vol / (maxVol || 1)) * (volumeChartH * 0.85);

    // 1. Draw Grid lines & Right Y-Axis Scale
    ctx.strokeStyle = '#16223a';
    ctx.lineWidth = 1;
    ctx.font = '10px "SF Mono", monospace';
    ctx.textAlign = 'left';
    ctx.fillStyle = '#8492a6';

    const numGridLines = 6;
    for (let i = 0; i <= numGridLines; i++) {
      const gridY = (mainChartH / numGridLines) * i;
      ctx.beginPath();
      ctx.moveTo(0, gridY);
      ctx.lineTo(chartW, gridY);
      ctx.stroke();

      const priceVal = maxPrice - (i / numGridLines) * priceRange;
      ctx.fillText(`$${priceVal.toFixed(2)}`, chartW + 6, gridY + 3);
    }

    // Timestamp labels along bottom X-axis
    ctx.fillStyle = '#64748b';
    ctx.font = '9px "SF Mono", monospace';
    ctx.textAlign = 'center';
    const timeStep = Math.max(1, Math.floor(visibleCandles.length / 6));
    for (let i = 0; i < visibleCandles.length; i += timeStep) {
      const c = visibleCandles[i];
      const x = getX(i);
      if (x > 20 && x < chartW - 20) {
        const d = new Date(c.timestamp);
        let timeStr = '';
        if (state.timeframe === '1s') {
          timeStr = d.toTimeString().split(' ')[0];
        } else if (state.timeframe === '1d') {
          timeStr = `${d.getMonth() + 1}/${d.getDate()}`;
        } else {
          timeStr = d.toTimeString().slice(0, 5);
        }
        ctx.fillText(timeStr, x, h - 6);
      }
    }

    // Divider between candles & volume
    ctx.strokeStyle = '#1c2a47';
    ctx.beginPath();
    ctx.moveTo(0, mainChartH);
    ctx.lineTo(w, mainChartH);
    ctx.stroke();

    // 2. Draw Technical Overlays (Bollinger Bands, VWAP)
    if (DOM.chkBollinger && DOM.chkBollinger.checked && state.indicators.bollinger_bands) {
      const bb = state.indicators.bollinger_bands;
      if (bb.upper && bb.lower) {
        const upperY = getY(bb.upper);
        const lowerY = getY(bb.lower);

        ctx.fillStyle = 'rgba(236, 72, 153, 0.06)';
        ctx.fillRect(0, upperY, chartW, Math.max(1, lowerY - upperY));

        ctx.strokeStyle = 'rgba(236, 72, 153, 0.4)';
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(0, upperY);
        ctx.lineTo(chartW, upperY);
        ctx.moveTo(0, lowerY);
        ctx.lineTo(chartW, lowerY);
        ctx.stroke();
        ctx.setLineDash([]);
      }
    }

    if (DOM.chkVwap && DOM.chkVwap.checked && state.indicators.vwap) {
      const vwapY = getY(state.indicators.vwap);
      ctx.strokeStyle = 'rgba(168, 85, 247, 0.7)';
      ctx.setLineDash([6, 4]);
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(0, vwapY);
      ctx.lineTo(chartW, vwapY);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // 3. Draw Candlesticks & Volume Bars
    visibleCandles.forEach((c, idx) => {
      const x = getX(idx);
      if (x < -candleWidth || x > chartW + candleWidth) return;

      const isUp = c.close >= c.open;
      const color = isUp ? '#00f076' : '#ff3b57';

      // Volume bar
      const volY = getVolY(c.volume);
      ctx.fillStyle = isUp ? 'rgba(0, 240, 118, 0.35)' : 'rgba(255, 59, 87, 0.35)';
      ctx.fillRect(x - candleWidth / 2, volY, candleWidth, Math.max(1, (h - marginBottom) - volY));

      // Candlestick wick
      const openY = getY(c.open);
      const closeY = getY(c.close);
      const highY = getY(c.high);
      const lowY = getY(c.low);

      ctx.strokeStyle = color;
      ctx.lineWidth = 1.2;
      ctx.beginPath();
      ctx.moveTo(x, highY);
      ctx.lineTo(x, lowY);
      ctx.stroke();

      // Candlestick body
      const bodyTop = Math.min(openY, closeY);
      const bodyH = Math.max(2, Math.abs(closeY - openY));
      ctx.fillStyle = color;
      ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyH);
    });

    // 4. Draw Current Price Line & Badge
    if (state.price) {
      const curY = getY(state.price);
      ctx.strokeStyle = '#00e5ff';
      ctx.lineWidth = 1;
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(0, curY);
      ctx.lineTo(chartW, curY);
      ctx.stroke();
      ctx.setLineDash([]);

      // Badge on Y-axis
      ctx.fillStyle = '#00e5ff';
      ctx.fillRect(chartW + 2, curY - 9, marginRight - 4, 18);
      ctx.fillStyle = '#000';
      ctx.font = 'bold 10px "SF Mono", monospace';
      ctx.textAlign = 'left';
      ctx.fillText(`$${state.price.toFixed(2)}`, chartW + 5, curY + 4);
    }

    // 5. Stream Paused Banner Watermark
    if (!state.simRunning) {
      ctx.save();
      ctx.fillStyle = 'rgba(245, 158, 11, 0.9)';
      ctx.fillRect(12, 12, 140, 24);
      ctx.fillStyle = '#0f172a';
      ctx.font = 'bold 11px system-ui, sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('⏸ STREAM PAUSED', 82, 28);
      ctx.restore();
    }

    // 6. Interactive Crosshairs & Tooltip
    if (chartState.crosshairX !== null && chartState.crosshairX < chartW) {
      const cx = chartState.crosshairX;
      const cy = chartState.crosshairY;

      ctx.strokeStyle = 'rgba(255, 255, 255, 0.4)';
      ctx.setLineDash([2, 2]);
      ctx.beginPath();
      ctx.moveTo(cx, 0);
      ctx.lineTo(cx, h - marginBottom);
      ctx.moveTo(0, cy);
      ctx.lineTo(chartW, cy);
      ctx.stroke();
      ctx.setLineDash([]);

      // Find closest candle to hover X
      let closestCandle = null;
      let minDist = Infinity;
      visibleCandles.forEach((c, idx) => {
        const x = getX(idx);
        const dist = Math.abs(x - cx);
        if (dist < minDist && dist <= slotWidth) {
          minDist = dist;
          closestCandle = c;
        }
      });

      if (closestCandle) {
        DOM.hoverO.textContent = closestCandle.open.toFixed(2);
        DOM.hoverH.textContent = closestCandle.high.toFixed(2);
        DOM.hoverL.textContent = closestCandle.low.toFixed(2);
        DOM.hoverC.textContent = closestCandle.close.toFixed(2);
        DOM.hoverV.textContent = closestCandle.volume.toFixed(4);

        // Hover price label on Y-axis
        const hoverPrice = maxPrice - (cy / mainChartH) * priceRange;
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(chartW + 2, cy - 8, marginRight - 4, 16);
        ctx.fillStyle = '#000000';
        ctx.font = '10px "SF Mono", monospace';
        ctx.fillText(`$${hoverPrice.toFixed(2)}`, chartW + 5, cy + 4);
      }
    }

    ctx.restore();
  }
})();

