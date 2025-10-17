use crate::{
    error::{WalError, WalResult},
    wal_event::{RawWalEvent, WalEventHandler},
};
use bytes::{Buf, Bytes};
use futures::StreamExt;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{Client, Config, NoTls};

/// Configuration for the WAL subscriber
#[derive(Debug, Clone)]
pub struct WalSubscriberConfig {
    /// Database connection string (must include replication=database parameter)
    /// Example: "host=localhost user=myuser password=mypass dbname=mydb replication=database"
    pub connection_string: String,

    /// Name of the replication slot to use
    pub slot_name: String,

    /// Name of the publication to subscribe to
    pub publication_name: String,

    /// Protocol version (use "1" for pgoutput)
    pub protocol_version: String,

    /// Whether to create the publication if it doesn't exist
    pub create_publication: bool,

    /// Tables to include in the publication (if creating)
    pub publication_tables: Vec<String>,
}

impl Default for WalSubscriberConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            slot_name: "cruding_wal_slot".to_string(),
            publication_name: "cruding_publication".to_string(),
            protocol_version: "1".to_string(),
            create_publication: true,
            publication_tables: Vec::new(),
        }
    }
}

/// Main WAL subscriber that connects to Postgres and streams replication events
pub struct WalSubscriber {
    config: WalSubscriberConfig,
    handlers: HashMap<String, Arc<dyn WalEventHandler>>,
}

impl WalSubscriber {
    /// Create a new WAL subscriber with the given configuration
    pub fn new(config: WalSubscriberConfig) -> Self {
        Self {
            config,
            handlers: HashMap::new(),
        }
    }

    /// Register a handler for a specific table
    pub fn register_handler(&mut self, handler: Arc<dyn WalEventHandler>) {
        let table_name = handler.table_name().to_string();
        tracing::info!("Registering WAL handler for table: {}", table_name);
        self.handlers.insert(table_name, handler);
    }

    /// Start the WAL subscriber (this will run indefinitely)
    pub async fn start(&mut self) -> WalResult<()> {
        tracing::info!("Starting WAL subscriber");

        // Connect to database
        let (client, connection) = self.connect_to_db().await?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Connection error: {}", e);
            }
        });

        // Setup replication
        self.setup_replication(&client).await?;

        // Start streaming
        self.stream_wal(client).await
    }

    /// Connect to the database with replication mode
    async fn connect_to_db(
        &self,
    ) -> WalResult<(
        Client,
        impl std::future::Future<Output = Result<(), tokio_postgres::Error>>,
    )> {
        tracing::info!("Connecting to database: {}", self.config.connection_string);

        let config: Config = self
            .config
            .connection_string
            .parse()
            .map_err(|e| WalError::ConfigError(format!("Invalid connection string: {}", e)))?;

        let (client, connection) = config.connect(NoTls).await?;

        tracing::info!("Connected to database successfully");
        Ok((client, connection))
    }

    /// Setup replication slot and publication
    async fn setup_replication(&mut self, client: &Client) -> WalResult<()> {
        // Create publication if needed
        if self.config.create_publication && !self.config.publication_tables.is_empty() {
            self.create_publication(client).await?;
        }

        // Create replication slot if it doesn't exist
        self.create_replication_slot(client).await?;

        Ok(())
    }

    /// Create the publication for the specified tables
    async fn create_publication(&self, client: &Client) -> WalResult<()> {
        let tables = self.config.publication_tables.join(", ");
        let query = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            self.config.publication_name, tables
        );

        tracing::info!("Creating publication: {}", query);

        match client.simple_query(&query).await {
            Ok(_) => {
                tracing::info!("Publication created successfully");
                Ok(())
            }
            Err(e) => {
                // If publication already exists, that's fine
                if e.to_string().contains("already exists") {
                    tracing::info!("Publication already exists");
                    Ok(())
                } else {
                    Err(WalError::PublicationError(format!(
                        "Failed to create publication: {}",
                        e
                    )))
                }
            }
        }
    }

    /// Create the replication slot if it doesn't exist
    async fn create_replication_slot(&self, client: &Client) -> WalResult<()> {
        let query = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput NOEXPORT_SNAPSHOT",
            self.config.slot_name
        );

        tracing::info!("Creating replication slot: {}", self.config.slot_name);

        match client.simple_query(&query).await {
            Ok(result) => {
                tracing::info!("Replication slot created: {:?}", result);
                Ok(())
            }
            Err(e) => {
                // If slot already exists, that's fine
                if e.to_string().contains("already exists") {
                    tracing::info!("Replication slot already exists");
                    Ok(())
                } else {
                    Err(WalError::ReplicationSlotError(format!(
                        "Failed to create replication slot: {}",
                        e
                    )))
                }
            }
        }
    }

    /// Start streaming WAL events
    async fn stream_wal(&mut self, client: Client) -> WalResult<()> {
        // Build the START_REPLICATION command
        let query = format!(
            r#"START_REPLICATION SLOT {} LOGICAL 0/0 (proto_version '{}', publication_names '{}')"#,
            self.config.slot_name, self.config.protocol_version, self.config.publication_name
        );

        tracing::info!("Starting replication stream");

        // Use copy_out to get the replication stream
        let copy_stream = client
            .copy_out(&query)
            .await
            .map_err(|e| WalError::ConnectionError(e))?;

        tokio::pin!(copy_stream);

        tracing::info!("Connected to replication stream, waiting for events...");

        // Process messages as they arrive
        while let Some(result) = copy_stream.next().await {
            match result {
                Ok(bytes) => {
                    if let Err(e) = self.process_replication_message(&bytes).await {
                        tracing::error!("Error processing replication message: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Error in replication stream: {}", e);
                    return Err(WalError::ConnectionError(e));
                }
            }
        }

        tracing::warn!("Replication stream ended");
        Ok(())
    }

    /// Process a replication message
    async fn process_replication_message(&mut self, data: &Bytes) -> WalResult<()> {
        // Parse the replication message format
        // Format: message_type (u8) + data
        
        if data.is_empty() {
            return Ok(());
        }

        let mut buf = data.clone();
        let message_type = buf.get_u8();

        match message_type {
            b'w' => {
                // XLogData message
                // Skip: wal_start (u64), wal_end (u64), timestamp (i64)
                if buf.remaining() < 24 {
                    return Ok(());
                }
                buf.advance(24);
                
                // The rest is the actual WAL data
                self.process_wal_data(&buf[..]).await
            }
            b'k' => {
                // Primary keepalive message
                tracing::trace!("Received keepalive");
                Ok(())
            }
            _ => {
                tracing::trace!("Unknown message type: {}", message_type as char);
                Ok(())
            }
        }
    }

    /// Process WAL data (logical replication messages)
    async fn process_wal_data(&mut self, data: &[u8]) -> WalResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        // Parse logical replication message
        // This is a simplified parser - in production you'd use postgres-protocol
        let message_type = data[0] as char;

        match message_type {
            'B' => {
                // Begin transaction
                tracing::debug!("Transaction begin");
                Ok(())
            }
            'C' => {
                // Commit transaction
                tracing::debug!("Transaction commit");
                Ok(())
            }
            'R' => {
                // Relation message (table metadata)
                self.parse_relation_message(&data[1..]).await
            }
            'I' => {
                // Insert message
                self.parse_insert_message(&data[1..]).await
            }
            'U' => {
                // Update message  
                self.parse_update_message(&data[1..]).await
            }
            'D' => {
                // Delete message
                self.parse_delete_message(&data[1..]).await
            }
            _ => {
                tracing::trace!("Unknown WAL message type: {}", message_type);
                Ok(())
            }
        }
    }

    /// Parse relation (table metadata) message
    async fn parse_relation_message(&mut self, _data: &[u8]) -> WalResult<()> {
        // This is a simplified parser
        // In production, use postgres_protocol::message::backend::LogicalReplicationMessage
        
        tracing::debug!("Received relation message (table metadata)");
        
        // For now, just acknowledge we got it
        // Full implementation would parse relation ID, schema, table name, columns
        Ok(())
    }

    /// Parse insert message
    async fn parse_insert_message(&mut self, _data: &[u8]) -> WalResult<()> {
        tracing::info!("📨 Received INSERT event");
        
        // Simplified: create a dummy event
        // Full implementation would parse the actual data
        
        Ok(())
    }

    /// Parse update message
    async fn parse_update_message(&mut self, _data: &[u8]) -> WalResult<()> {
        tracing::info!("📨 Received UPDATE event");
        Ok(())
    }

    /// Parse delete message
    async fn parse_delete_message(&mut self, _data: &[u8]) -> WalResult<()> {
        tracing::info!("📨 Received DELETE event");
        Ok(())
    }

    /// Dispatch an event to the appropriate handler
    async fn _dispatch_event(&self, event: RawWalEvent) -> WalResult<()> {
        if let Some(handler) = self.handlers.get(&event.table_name) {
            tracing::debug!(
                "Dispatching {:?} event for table: {}",
                event.operation,
                event.table_name
            );

            handler.handle(event).await.map_err(|e| {
                WalError::HandlerError(format!("Handler failed: {}", e))
            })?;
        } else {
            tracing::trace!(
                "No handler registered for table: {}",
                event.table_name
            );
        }

        Ok(())
    }
}