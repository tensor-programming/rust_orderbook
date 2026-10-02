//! Shared domain types and error definitions for the trading engine.
//!
//! This crate provides the core types used across all rust-services crates,
//! ensuring a consistent domain model. All types implement `Serialize` and
//! `Deserialize` for seamless JSON interop with external systems (e.g. server.js).

pub mod errors;
pub mod types;

pub use errors::*;
pub use types::*;
