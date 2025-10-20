#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOperation {
    Insert,
    Update,
    Delete,
}

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

#[derive(Debug, Clone)]
pub struct RawWalEvent {
    pub table_name: String,
    pub schema_name: String,
    pub operation: WalOperation,
    pub old_tuple: Option<Vec<u8>>,
    pub new_tuple: Option<Vec<u8>>,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_id: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone)]
pub enum TupleValue {
    Null,
    Text(String),
    Binary(Vec<u8>),
}

impl RawWalEvent {
    pub fn is_table(&self, table: &str) -> bool {
        self.table_name == table
    }

    pub fn is_insert(&self) -> bool {
        matches!(self.operation, WalOperation::Insert)
    }

    pub fn is_update(&self) -> bool {
        matches!(self.operation, WalOperation::Update)
    }

    pub fn is_delete(&self) -> bool {
        matches!(self.operation, WalOperation::Delete)
    }
}

#[async_trait::async_trait]
pub trait WalEventHandler: Send + Sync {
    async fn handle(&self, event: RawWalEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn table_name(&self) -> &str;
}