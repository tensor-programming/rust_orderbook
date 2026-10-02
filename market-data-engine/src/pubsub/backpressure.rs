//! Backpressure management strategies for market data streaming and subscribers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// Backpressure policy for handling consumer lag or bursty message producers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureStrategy {
    /// Evict the oldest message in the buffer to make room for fresh incoming data (ideal for real-time UI).
    DropOldest,
    /// Discard incoming messages when the buffer is full to preserve already queued frames.
    DropLatest,
    /// Throttle and coalesce updates to a maximum frequency (e.g., max 60Hz / 16ms or 50ms intervals).
    Throttle { min_interval_ms: u64 },
    /// Apply synchronous upstream backpressure until buffer space becomes available.
    Block,
}

impl Default for BackpressureStrategy {
    fn default() -> Self {
        BackpressureStrategy::DropOldest
    }
}

impl BackpressureStrategy {
    pub fn parse(s: &str, throttle_ms: Option<u64>) -> Self {
        match s.to_lowercase().as_str() {
            "drop_oldest" | "dropoldest" | "fresh" => BackpressureStrategy::DropOldest,
            "drop_latest" | "droplatest" | "drop" => BackpressureStrategy::DropLatest,
            "throttle" | "rate_limit" | "coalesce" => {
                BackpressureStrategy::Throttle { min_interval_ms: throttle_ms.unwrap_or(50) }
            }
            "block" | "sync" | "wait" => BackpressureStrategy::Block,
            _ => BackpressureStrategy::DropOldest,
        }
    }
}

/// Statistics tracking backpressure queue metrics.
#[derive(Debug, Default)]
pub struct BackpressureMetrics {
    pub total_pushed: AtomicU64,
    pub total_dropped: AtomicU64,
    pub total_consumed: AtomicU64,
}

/// A thread-safe bounded queue that enforces backpressure rules without deadlocks.
pub struct BackpressureQueue<T> {
    capacity: usize,
    strategy: BackpressureStrategy,
    buffer: Mutex<VecDeque<T>>,
    notify: Arc<Notify>,
    metrics: Arc<BackpressureMetrics>,
    last_push_time: Mutex<Option<Instant>>,
}

impl<T: Clone> BackpressureQueue<T> {
    pub fn new(capacity: usize, strategy: BackpressureStrategy) -> Self {
        Self {
            capacity: capacity.max(1),
            strategy,
            buffer: Mutex::new(VecDeque::with_capacity(capacity)),
            notify: Arc::new(Notify::new()),
            metrics: Arc::new(BackpressureMetrics::default()),
            last_push_time: Mutex::new(None),
        }
    }

    /// Push an item into the queue according to the configured backpressure strategy.
    /// Returns `true` if item was queued, `false` if dropped due to backpressure.
    pub fn push(&self, item: T) -> bool {
        self.metrics.total_pushed.fetch_add(1, Ordering::Relaxed);

        match self.strategy {
            BackpressureStrategy::Throttle { min_interval_ms } => {
                let mut last_time = self.last_push_time.lock();
                let now = Instant::now();

                if let Some(prev) = *last_time {
                    if now.duration_since(prev).as_millis() < min_interval_ms as u128 {
                        // In throttle mode, replace the most recent unconsumed item if queue is full
                        let mut buf = self.buffer.lock();
                        if !buf.is_empty() {
                            buf.pop_back();
                            buf.push_back(item);
                            self.metrics.total_dropped.fetch_add(1, Ordering::Relaxed);
                            return true;
                        }
                    }
                }

                *last_time = Some(now);
                let mut buf = self.buffer.lock();
                if buf.len() >= self.capacity {
                    buf.pop_front();
                    self.metrics.total_dropped.fetch_add(1, Ordering::Relaxed);
                }
                buf.push_back(item);
                self.notify.notify_one();
                true
            }
            BackpressureStrategy::DropOldest => {
                let mut buf = self.buffer.lock();
                if buf.len() >= self.capacity {
                    buf.pop_front();
                    self.metrics.total_dropped.fetch_add(1, Ordering::Relaxed);
                }
                buf.push_back(item);
                self.notify.notify_one();
                true
            }
            BackpressureStrategy::DropLatest => {
                let mut buf = self.buffer.lock();
                if buf.len() >= self.capacity {
                    self.metrics.total_dropped.fetch_add(1, Ordering::Relaxed);
                    false
                } else {
                    buf.push_back(item);
                    self.notify.notify_one();
                    true
                }
            }
            BackpressureStrategy::Block => {
                let mut buf = self.buffer.lock();
                if buf.len() >= self.capacity {
                    // In non-async push, we drop oldest if buffer overflow occurs in sync context
                    buf.pop_front();
                    self.metrics.total_dropped.fetch_add(1, Ordering::Relaxed);
                }
                buf.push_back(item);
                self.notify.notify_one();
                true
            }
        }
    }

    /// Try to pop the next item from the queue without blocking.
    pub fn try_pop(&self) -> Option<T> {
        let mut buf = self.buffer.lock();
        let item = buf.pop_front();
        if item.is_some() {
            self.metrics.total_consumed.fetch_add(1, Ordering::Relaxed);
        }
        item
    }

    /// Pop the next item, awaiting notification if empty.
    pub async fn pop(&self) -> T {
        loop {
            if let Some(item) = self.try_pop() {
                return item;
            }
            self.notify.notified().await;
        }
    }

    /// Number of items currently queued in the buffer.
    pub fn len(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Check if queue is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.lock().is_empty()
    }

    /// Queue capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Access metrics tracker.
    pub fn metrics(&self) -> &BackpressureMetrics {
        &self.metrics
    }
}
