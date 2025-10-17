/// Represents a WAL operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOperation {
    Insert,
    Update,
    Delete,
}

/// Represents the transaction state
#[derive(Debug, Clone)]
pub enum TransactionEvent {
    Begin { 
        final_lsn: u64,
        commit_time: i64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
        commit_time: i64,
    },
}

/// Raw WAL event before parsing into specific types
#[derive(Debug, Clone)]
pub struct RawWalEvent {
    /// Name of the table that changed
    pub table_name: String,
    
    /// Schema name
    pub schema_name: String,
    
    /// The operation type
    pub operation: WalOperation,
    
    /// Old tuple data (for UPDATE and DELETE)
    pub old_tuple: Option<Vec<u8>>,
    
    /// New tuple data (for INSERT and UPDATE)
    pub new_tuple: Option<Vec<u8>>,
    
    /// Column names in order
    pub columns: Vec<ColumnInfo>,
}

/// Information about a column in the relation
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_id: u32,
    pub type_modifier: i32,
}

/// A parsed tuple value
#[derive(Debug, Clone)]
pub enum TupleValue {
    Null,
    Text(String),
    Binary(Vec<u8>),
}

impl RawWalEvent {
    /// Helper to check if this event is for a specific table
    pub fn is_table(&self, table: &str) -> bool {
        self.table_name == table
    }

    /// Helper to check if this is an insert operation
    pub fn is_insert(&self) -> bool {
        matches!(self.operation, WalOperation::Insert)
    }

    /// Helper to check if this is an update operation
    pub fn is_update(&self) -> bool {
        matches!(self.operation, WalOperation::Update)
    }

    /// Helper to check if this is a delete operation
    pub fn is_delete(&self) -> bool {
        matches!(self.operation, WalOperation::Delete)
    }
}

/// Trait that must be implemented to handle WAL events for a specific table
#[async_trait::async_trait]
pub trait WalEventHandler: Send + Sync {
    /// Handle a raw WAL event
    async fn handle(&self, event: RawWalEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get the table name this handler is for
    fn table_name(&self) -> &str;
}