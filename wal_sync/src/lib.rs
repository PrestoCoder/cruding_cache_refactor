//! # cruding-wal
//!
//! PostgreSQL Write-Ahead Log (WAL) subscriber for the Cruding framework.
//!
//! This crate provides functionality to subscribe to PostgreSQL logical replication
//! and process WAL events in real-time, enabling automatic cache synchronization
//! and event-driven architectures.
//!
//! ## Features
//!
//! - Subscribe to PostgreSQL logical replication stream
//! - Decode WAL events using the pgoutput protocol
//! - Type-safe event handling for different tables
//! - Automatic replication slot and publication management
//! - Integration with the Cruding framework
//!
//! ## Example
//!
//! ```rust,no_run
//! use cruding_wal::{WalSubscriber, WalSubscriberConfig, WalEventHandler, RawWalEvent};
//! use std::sync::Arc;
//!
//! struct MyTableHandler;
//!
//! #[async_trait::async_trait]
//! impl WalEventHandler for MyTableHandler {
//!     async fn handle(&self, event: RawWalEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!         println!("Received event for table: {}", event.table_name);
//!         Ok(())
//!     }
//!     
//!     fn table_name(&self) -> &str {
//!         "my_table"
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = WalSubscriberConfig {
//!         connection_string: "host=localhost user=postgres dbname=mydb replication=database".to_string(),
//!         slot_name: "my_slot".to_string(),
//!         publication_name: "my_publication".to_string(),
//!         publication_tables: vec!["my_table".to_string()],
//!         ..Default::default()
//!     };
//!
//!     let mut subscriber = WalSubscriber::new(config);
//!     subscriber.register_handler(Arc::new(MyTableHandler));
//!     
//!     subscriber.start().await.expect("Failed to start WAL subscriber");
//! }
//! ```

pub mod error;
pub mod tuple_parser;
pub mod wal_event;
pub mod wal_subscriber;

pub use error::{WalError, WalResult};
pub use tuple_parser::TupleParser;
pub use wal_event::{
    ColumnInfo, RawWalEvent, TransactionEvent, TupleValue, WalEventHandler, WalOperation,
};
pub use wal_subscriber::{WalSubscriber, WalSubscriberConfig};
