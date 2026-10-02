//! Engine error types.

use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur during order processing and matching.
#[derive(Error, Debug)]
pub enum EngineError {
    /// The specified order was not found in the book.
    #[error("Order not found: {0}")]
    OrderNotFound(Uuid),

    /// The order failed validation.
    #[error("Invalid order: {0}")]
    InvalidOrder(String),

    /// No order book exists for the given symbol.
    #[error("Book not found for symbol: {0}")]
    BookNotFound(String),

    /// A catch-all for unexpected internal errors.
    #[error("Internal engine error: {0}")]
    Internal(String),
}
