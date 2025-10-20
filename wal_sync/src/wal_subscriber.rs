use crate::{
    error::{WalError, WalResult},
    wal_event::{RawWalEvent, WalEventHandler},
};
use bytes::{Buf, Bytes};
use futures::StreamExt;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{Client, Config, NoTls};

#[derive(Debug, Clone)]
pub struct DatabaseCredentials {
    pub host: String,
    pub user: String,
    pub password: String,
    pub db_name: String,
}

impl DatabaseCredentials {
    pub fn new() -> Self {
        Self {
            host: "localhost".to_string(),
            user: "postgres".to_string(),
            password: "".to_string(),
            db_name: "".to_string(),
        }
    }

    pub fn to_connection_string(&self) -> String {
        format!(
            "host={} user={} password={} dbname={} replication=database",
            self.host, self.user, self.password, self.db_name
        )
    }
}

#[derive(Debug, Clone)]
pub struct WalSubscriberConfig {
    pub database_creds: DatabaseCredentials,
    pub slot_name: String,
    pub publication_name: String,
    /// Protocol version (use "1" for pgoutput)
    pub protocol_version: String,
    pub create_publication: bool,
    pub publication_tables: Vec<String>,
}

impl Default for WalSubscriberConfig {
    fn default() -> Self {
        Self {
            database_creds: DatabaseCredentials::new(),
            slot_name: "cruding_wal_slot".to_string(),
            publication_name: "cruding_publication".to_string(),
            protocol_version: "1".to_string(),
            create_publication: true,
            publication_tables: Vec::new(),
        }
    }
}

pub struct WalSubscriber {
    config: WalSubscriberConfig,
    handlers: HashMap<String, Arc<dyn WalEventHandler>>,
}

impl WalSubscriber {
    pub fn new(config: WalSubscriberConfig) -> Self {
        Self {
            config,
            handlers: HashMap::new(),
        }
    }

    pub fn register_handler(&mut self, handler: Arc<dyn WalEventHandler>) {
        let table_name = handler.table_name().to_string();
        tracing::info!("Registering WAL handler for table: {}", table_name);
        self.handlers.insert(table_name, handler);
    }

    pub async fn start(&mut self) -> WalResult<()> {
        tracing::info!("Starting WAL subscriber");

        let (client, connection) = self.connect_to_db().await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Connection error: {}", e);
            }
        });

        self.setup_replication(&client).await?;
        self.stream_wal(client).await
    }

    async fn connect_to_db(
        &self,
    ) -> WalResult<(
        Client,
        impl std::future::Future<Output = Result<(), tokio_postgres::Error>>,
    )> {
        tracing::info!(
            "Connecting to database: {} at {}",
            self.config.database_creds.db_name,
            self.config.database_creds.host
        );

        let connection_string = self.config.database_creds.to_connection_string();

        let config: Config = connection_string.parse().map_err(|e| {
            WalError::ConfigError(format!("Invalid connection string: {}", e))
        })?;

        let (client, connection) = config.connect(NoTls).await?;

        tracing::info!("Connected to database successfully");
        Ok((client, connection))
    }

    async fn setup_replication(&mut self, client: &Client) -> WalResult<()> {
        if self.config.create_publication && !self.config.publication_tables.is_empty() {
            self.create_publication(client).await?;
        }

        self.create_replication_slot(client).await?;

        Ok(())
    }

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

    async fn stream_wal(&mut self, client: Client) -> WalResult<()> {
        let query = format!(
            r#"START_REPLICATION SLOT {} LOGICAL 0/0 (proto_version '{}', publication_names '{}')"#,
            self.config.slot_name, self.config.protocol_version, self.config.publication_name
        );

        tracing::info!("Starting replication stream");

        let copy_stream = client
            .copy_out(&query)
            .await
            .map_err(|e| WalError::ConnectionError(e))?;

        tokio::pin!(copy_stream);

        tracing::info!("Connected to replication stream, waiting for events...");

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

    async fn process_replication_message(&mut self, data: &Bytes) -> WalResult<()> {
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

        // This is a simplified parser - in production you'd use postgres-protocol
        let message_type = data[0] as char;

        match message_type {
            'B' => {
                tracing::debug!("Transaction begin");
                Ok(())
            }
            'C' => {
                tracing::debug!("Transaction commit");
                Ok(())
            }
            'R' => {
                self.parse_relation_message(&data[1..]).await
            }
            'I' => {
                self.parse_insert_message(&data[1..]).await
            }
            'U' => {
                self.parse_update_message(&data[1..]).await
            }
            'D' => {
                self.parse_delete_message(&data[1..]).await
            }
            _ => {
                tracing::trace!("Unknown WAL message type: {}", message_type);
                Ok(())
            }
        }
    }


    async fn parse_relation_message(&mut self, _data: &[u8]) -> WalResult<()> {
        tracing::debug!("Received relation message (table metadata)");
        Ok(())
    }

    async fn parse_insert_message(&mut self, _data: &[u8]) -> WalResult<()> {
        tracing::info!("📨 Received INSERT event");
        
        Ok(())
    }

    async fn parse_update_message(&mut self, _data: &[u8]) -> WalResult<()> {
        tracing::info!("📨 Received UPDATE event");
        Ok(())
    }

    async fn parse_delete_message(&mut self, _data: &[u8]) -> WalResult<()> {
        tracing::info!("📨 Received DELETE event");
        Ok(())
    }

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