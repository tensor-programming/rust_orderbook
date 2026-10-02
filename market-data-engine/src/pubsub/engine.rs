//! In-memory, high-throughput Market Data PubSub Broker.

use std::sync::Arc;

use dashmap::DashMap;
use tracing::debug;
use uuid::Uuid;

use super::backpressure::{BackpressureQueue, BackpressureStrategy};
use crate::messages::ServerMessage;
use crate::subscription::{Channel, SubscriptionManager, Topic};

/// Central PubSub Broker decoupling data producers from consumers.
pub struct MarketDataPubSub {
    subscriptions: Arc<SubscriptionManager>,
    /// client_id -> BackpressureQueue
    client_queues: DashMap<Uuid, Arc<BackpressureQueue<ServerMessage>>>,
}

impl MarketDataPubSub {
    pub fn new(subscriptions: Arc<SubscriptionManager>) -> Self {
        Self {
            subscriptions,
            client_queues: DashMap::new(),
        }
    }

    /// Register a subscriber with a custom backpressure strategy and queue capacity.
    pub fn register_subscriber(
        &self,
        client_id: Uuid,
        capacity: usize,
        strategy: BackpressureStrategy,
    ) -> Arc<BackpressureQueue<ServerMessage>> {
        let queue = Arc::new(BackpressureQueue::new(capacity, strategy));
        self.client_queues.insert(client_id, Arc::clone(&queue));
        self.subscriptions.register_client(client_id);
        queue
    }

    /// Unregister a subscriber and remove all its topic subscriptions.
    pub fn unregister_subscriber(&self, client_id: &Uuid) {
        self.client_queues.remove(client_id);
        self.subscriptions.unregister_client(client_id);
    }

    /// Subscribe an existing client to a topic.
    pub fn subscribe(&self, client_id: &Uuid, topic: Topic) {
        self.subscriptions.subscribe(client_id, topic);
    }

    /// Unsubscribe an existing client from a topic.
    pub fn unsubscribe(&self, client_id: &Uuid, topic: &Topic) {
        self.subscriptions.unsubscribe(client_id, topic);
    }

    /// Publish a market data event to all subscribers matching the target and channel.
    pub fn publish(&self, target: &str, channel: Channel, message: ServerMessage) {
        let subscribers = self.subscriptions.get_subscribers_for(target, channel);

        if subscribers.is_empty() {
            return;
        }

        for client_id in &subscribers {
            if let Some(queue) = self.client_queues.get(client_id) {
                if !queue.push(message.clone()) {
                    debug!(%client_id, "Frame dropped on client backpressure queue");
                }
            }
        }
    }

    /// Get subscriber queue for a client.
    pub fn get_subscriber_queue(
        &self,
        client_id: &Uuid,
    ) -> Option<Arc<BackpressureQueue<ServerMessage>>> {
        self.client_queues.get(client_id).map(|q| Arc::clone(&q))
    }

    /// Total number of active subscribers.
    pub fn active_subscriber_count(&self) -> usize {
        self.client_queues.len()
    }
}
