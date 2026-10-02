//! Multi-client topic and market subscription manager.

use std::collections::HashSet;
use dashmap::DashMap;
use uuid::Uuid;

use crate::aggregation::types::Timeframe;

/// Available market data subscription channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    All,
    OrderBook,
    EnrichedOrderBook,
    Trades,
    Prices,
    Candles(Option<Timeframe>),
    Indicators(Option<Timeframe>),
}

impl Channel {
    pub fn parse(s: &str, tf_opt: Option<&str>) -> Self {
        let timeframe = tf_opt.and_then(Timeframe::parse);
        match s.to_lowercase().as_str() {
            "orderbook" | "depth" | "book" => Channel::OrderBook,
            "orderbook_enriched" | "enriched_book" | "depth_analytics" => Channel::EnrichedOrderBook,
            "trades" | "trade" => Channel::Trades,
            "prices" | "price" | "ticker" => Channel::Prices,
            "candles" | "candle" | "kline" | "ohlc" => Channel::Candles(timeframe),
            "indicators" | "technical" => Channel::Indicators(timeframe),
            _ => Channel::All,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Channel::All => "all",
            Channel::OrderBook => "orderbook",
            Channel::EnrichedOrderBook => "orderbook_enriched",
            Channel::Trades => "trades",
            Channel::Prices => "prices",
            Channel::Candles(_) => "candles",
            Channel::Indicators(_) => "indicators",
        }
    }
}

/// A specific topic identifier (target symbol or marketId + channel).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Topic {
    pub target: String,
    pub channel: Channel,
}

impl Topic {
    pub fn new(target: impl Into<String>, channel: Channel) -> Self {
        Self {
            target: target.into().to_uppercase(),
            channel,
        }
    }

    pub fn matches(&self, target: &str, channel: &Channel) -> bool {
        let t_upper = target.to_uppercase();
        let target_matches = self.target == "*" || self.target == t_upper;
        
        let channel_matches = match (&self.channel, channel) {
            (Channel::All, _) => true,
            (Channel::Candles(None), Channel::Candles(_)) => true,
            (Channel::Candles(Some(tf1)), Channel::Candles(Some(tf2))) => tf1 == tf2,
            (Channel::Indicators(None), Channel::Indicators(_)) => true,
            (Channel::Indicators(Some(tf1)), Channel::Indicators(Some(tf2))) => tf1 == tf2,
            (c1, c2) => c1 == c2,
        };

        target_matches && channel_matches
    }
}

/// Thread-safe subscription manager for concurrent WebSocket clients.
pub struct SubscriptionManager {
    /// client_id -> set of subscribed topics
    client_subscriptions: DashMap<Uuid, HashSet<Topic>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            client_subscriptions: DashMap::new(),
        }
    }

    /// Register a new client session.
    pub fn register_client(&self, client_id: Uuid) {
        self.client_subscriptions.entry(client_id).or_default();
    }

    /// Remove a client and all its subscriptions upon disconnect.
    pub fn unregister_client(&self, client_id: &Uuid) {
        self.client_subscriptions.remove(client_id);
    }

    /// Add a topic subscription for a client.
    pub fn subscribe(&self, client_id: &Uuid, topic: Topic) {
        if let Some(mut subs) = self.client_subscriptions.get_mut(client_id) {
            subs.insert(topic);
        } else {
            let mut set = HashSet::new();
            set.insert(topic);
            self.client_subscriptions.insert(*client_id, set);
        }
    }

    /// Remove a topic subscription for a client.
    pub fn unsubscribe(&self, client_id: &Uuid, topic: &Topic) {
        if let Some(mut subs) = self.client_subscriptions.get_mut(client_id) {
            subs.remove(topic);
        }
    }

    /// Get all client IDs that match a given target and channel.
    pub fn get_subscribers_for(&self, target: &str, channel: Channel) -> Vec<Uuid> {
        self.get_subscribers_for_targets(&[target], channel)
    }

    /// Get all client IDs that match any of the given targets and channel.
    pub fn get_subscribers_for_targets(&self, targets: &[&str], channel: Channel) -> Vec<Uuid> {
        let mut matching = Vec::new();
        for entry in self.client_subscriptions.iter() {
            let client_id = entry.key();
            let topics = entry.value();
            if topics
                .iter()
                .any(|t| targets.iter().any(|target| t.matches(target, &channel)))
            {
                matching.push(*client_id);
            }
        }
        matching
    }

    /// Get total active client connections count.
    pub fn active_client_count(&self) -> usize {
        self.client_subscriptions.len()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}
