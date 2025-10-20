use crate::{
    error::{WalError, WalResult},
    wal_event::{ColumnInfo, RawWalEvent, WalEventHandler, WalOperation},
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

#[derive(Debug, Clone)]
struct RelationInfo {
    relation_id: u32,
    schema_name: String,
    table_name: String,
    columns: Vec<ColumnInfo>,
}

pub struct WalSubscriber {
    config: WalSubscriberConfig,
    handlers: HashMap<String, Arc<dyn WalEventHandler>>,
    relation_cache: HashMap<u32, RelationInfo>,
}

impl WalSubscriber {
    pub fn new(config: WalSubscriberConfig) -> Self {
        Self {
            config,
            handlers: HashMap::new(),
            relation_cache: HashMap::new(),
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
        if data.is_empty() {
            return Ok(());
        }

        let mut buf = data.clone();
        let message_type = buf.get_u8();

        match message_type {
            b'w' => {
                if buf.remaining() < 24 {
                    return Ok(());
                }
                buf.advance(24);
                self.process_wal_data(&buf[..]).await
            }
            b'k' => {
                tracing::trace!("Received keepalive");
                Ok(())
            }
            _ => {
                tracing::trace!("Unknown message type: {}", message_type as char);
                Ok(())
            }
        }
    }

    pub async fn process_wal_data(&mut self, data: &[u8]) -> WalResult<()> {
        if data.is_empty() {
            return Ok(());
        }

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
            'R' => self.parse_relation_message(&data[1..]).await,
            'I' => self.parse_insert_message(&data[1..]).await,
            'U' => self.parse_update_message(&data[1..]).await,
            'D' => self.parse_delete_message(&data[1..]).await,
            _ => {
                tracing::trace!("Unknown WAL message type: {}", message_type);
                Ok(())
            }
        }
    }

    async fn parse_relation_message(&mut self, data: &[u8]) -> WalResult<()> {
        let mut cursor = 0;

        let relation_id = read_u32(data, &mut cursor)?;
        let schema_name = read_string(data, &mut cursor)?;
        let table_name = read_string(data, &mut cursor)?;

        cursor += 1;

        let num_columns = read_u16(data, &mut cursor)?;
        let mut columns = Vec::with_capacity(num_columns as usize);

        for _ in 0..num_columns {
            cursor += 1;
            let col_name = read_string(data, &mut cursor)?;
            let type_id = read_u32(data, &mut cursor)?;
            let type_modifier = read_i32(data, &mut cursor)?;

            columns.push(ColumnInfo {
                name: col_name,
                type_id,
                type_modifier,
            });
        }

        let rel_info = RelationInfo {
            relation_id,
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
            columns,
        };

        tracing::info!(
            "Cached schema for table: {}.{} (relation_id: {})",
            schema_name,
            table_name,
            relation_id
        );

        self.relation_cache.insert(relation_id, rel_info);
        Ok(())
    }

    async fn parse_insert_message(&mut self, data: &[u8]) -> WalResult<()> {
        let mut cursor = 0;

        let relation_id = read_u32(data, &mut cursor)?;

        let tuple_type = data[cursor] as char;
        cursor += 1;

        if tuple_type != 'N' {
            return Err(WalError::DecodingError(format!(
                "Expected 'N' for new tuple, got '{}'",
                tuple_type
            )));
        }

        let relation = self.relation_cache.get(&relation_id).ok_or_else(|| {
            WalError::DecodingError(format!("Unknown relation ID: {}", relation_id))
        })?;

        let tuple_data = parse_tuple(&data[cursor..], relation.columns.len())?;

        let event = RawWalEvent {
            table_name: relation.table_name.clone(),
            schema_name: relation.schema_name.clone(),
            operation: WalOperation::Insert,
            old_tuple: None,
            new_tuple: Some(tuple_data),
            columns: relation.columns.clone(),
        };

        tracing::info!("📨 INSERT on table: {}", event.table_name);

        self.dispatch_event(event).await?;
        Ok(())
    }

    async fn parse_update_message(&mut self, data: &[u8]) -> WalResult<()> {
        let mut cursor = 0;

        let relation_id = read_u32(data, &mut cursor)?;

        let relation = self.relation_cache.get(&relation_id).ok_or_else(|| {
            WalError::DecodingError(format!("Unknown relation ID: {}", relation_id))
        })?;

        let tuple_type = data[cursor] as char;
        cursor += 1;

        let old_tuple = match tuple_type {
            'O' | 'K' => {
                // Parse old tuple
                let tuple_data = parse_tuple(&data[cursor..], relation.columns.len())?;
                let tuple_len = calculate_tuple_length(&data[cursor..], relation.columns.len())?;
                cursor += tuple_len;
                
                // After old tuple, we expect 'N' for new tuple
                if data[cursor] as char != 'N' {
                    return Err(WalError::DecodingError(
                        "Expected 'N' for new tuple in UPDATE".to_string(),
                    ));
                }
                cursor += 1; // Consume the 'N'
                
                Some(tuple_data)
            }
            'N' => {
                // No old tuple, 'N' was already consumed above
                // Cursor is now positioned at the start of new tuple data
                None
            }
            _ => {
                return Err(WalError::DecodingError(format!(
                    "Unexpected tuple type: '{}'",
                    tuple_type
                )))
            }
        };

        // Parse new tuple (cursor is already positioned correctly)
        let new_tuple = parse_tuple(&data[cursor..], relation.columns.len())?;

        let event = RawWalEvent {
            table_name: relation.table_name.clone(),
            schema_name: relation.schema_name.clone(),
            operation: WalOperation::Update,
            old_tuple,
            new_tuple: Some(new_tuple),
            columns: relation.columns.clone(),
        };

        tracing::info!("📨 UPDATE on table: {}", event.table_name);

        self.dispatch_event(event).await?;
        Ok(())
    }

    async fn parse_delete_message(&mut self, data: &[u8]) -> WalResult<()> {
        let mut cursor = 0;

        let relation_id = read_u32(data, &mut cursor)?;

        let tuple_type = data[cursor] as char;
        cursor += 1;

        if tuple_type != 'O' && tuple_type != 'K' {
            return Err(WalError::DecodingError(format!(
                "Expected 'O' or 'K' for DELETE, got '{}'",
                tuple_type
            )));
        }

        let relation = self.relation_cache.get(&relation_id).ok_or_else(|| {
            WalError::DecodingError(format!("Unknown relation ID: {}", relation_id))
        })?;

        let tuple_data = parse_tuple(&data[cursor..], relation.columns.len())?;

        let event = RawWalEvent {
            table_name: relation.table_name.clone(),
            schema_name: relation.schema_name.clone(),
            operation: WalOperation::Delete,
            old_tuple: Some(tuple_data),
            new_tuple: None,
            columns: relation.columns.clone(),
        };

        tracing::info!("📨 DELETE on table: {}", event.table_name);

        self.dispatch_event(event).await?;
        Ok(())
    }

    async fn dispatch_event(&self, event: RawWalEvent) -> WalResult<()> {
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

fn _read_u8(data: &[u8], cursor: &mut usize) -> WalResult<u8> {
    if *cursor >= data.len() {
        return Err(WalError::DecodingError("Unexpected end of data".to_string()));
    }
    let value = data[*cursor];
    *cursor += 1;
    Ok(value)
}

pub fn read_u16(data: &[u8], cursor: &mut usize) -> WalResult<u16> {
    if *cursor + 2 > data.len() {
        return Err(WalError::DecodingError("Unexpected end of data".to_string()));
    }
    let value = u16::from_be_bytes([data[*cursor], data[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

pub fn read_u32(data: &[u8], cursor: &mut usize) -> WalResult<u32> {
    if *cursor + 4 > data.len() {
        return Err(WalError::DecodingError("Unexpected end of data".to_string()));
    }
    let value = u32::from_be_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(value)
}

pub fn read_i32(data: &[u8], cursor: &mut usize) -> WalResult<i32> {
    if *cursor + 4 > data.len() {
        return Err(WalError::DecodingError("Unexpected end of data".to_string()));
    }
    let value = i32::from_be_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(value)
}

pub fn read_string(data: &[u8], cursor: &mut usize) -> WalResult<String> {
    let start = *cursor;
    while *cursor < data.len() && data[*cursor] != 0 {
        *cursor += 1;
    }
    if *cursor >= data.len() {
        return Err(WalError::DecodingError(
            "String not null-terminated".to_string(),
        ));
    }
    let string = std::str::from_utf8(&data[start..*cursor])
        .map_err(|e| WalError::DecodingError(format!("Invalid UTF-8: {}", e)))?
        .to_string();
    *cursor += 1;
    Ok(string)
}

pub fn parse_tuple(data: &[u8], num_columns: usize) -> WalResult<Vec<u8>> {
    let mut cursor = 0;
    let num_cols = read_u16(data, &mut cursor)?;

    if num_cols as usize != num_columns {
        return Err(WalError::DecodingError(format!(
            "Column count mismatch: expected {}, got {}",
            num_columns, num_cols
        )));
    }

    let mut result = Vec::new();

    for _ in 0..num_cols {
        let col_type = data[cursor] as char;
        cursor += 1;

        match col_type {
            'n' => {
                result.push(255);
            }
            't' => {
                let length = read_u32(data, &mut cursor)? as usize;
                if cursor + length > data.len() {
                    return Err(WalError::DecodingError("Tuple data truncated".to_string()));
                }
                result.extend_from_slice(&data[cursor..cursor + length]);
                result.push(0);
                cursor += length;
            }
            'u' => {
                result.push(254);
            }
            _ => {
                return Err(WalError::DecodingError(format!(
                    "Unknown column type: '{}'",
                    col_type
                )));
            }
        }
    }

    Ok(result)
}

pub fn calculate_tuple_length(data: &[u8], num_columns: usize) -> WalResult<usize> {
    let mut cursor = 0;
    let num_cols = read_u16(data, &mut cursor)?;

    if num_cols as usize != num_columns {
        return Err(WalError::DecodingError(format!(
            "Column count mismatch: expected {}, got {}",
            num_columns, num_cols
        )));
    }

    for _ in 0..num_cols {
        let col_type = data[cursor] as char;
        cursor += 1;

        match col_type {
            'n' => {}
            't' => {
                let length = read_u32(data, &mut cursor)? as usize;
                cursor += length;
            }
            'u' => {}
            _ => {
                return Err(WalError::DecodingError(format!(
                    "Unknown column type: '{}'",
                    col_type
                )));
            }
        }
    }

    Ok(cursor)
}

