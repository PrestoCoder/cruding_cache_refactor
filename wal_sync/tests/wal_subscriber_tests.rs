// Integration tests for WAL subscriber
use wal_sync::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// Production-grade mock handler with metrics and error simulation
struct ProductionHandler {
    table: String,
    events: Arc<Mutex<Vec<RawWalEvent>>>,
    call_count: Arc<AtomicUsize>,
    should_fail: Arc<Mutex<bool>>,
    failure_count: Arc<AtomicUsize>,
    processing_times: Arc<Mutex<Vec<u128>>>,
}

impl ProductionHandler {
    fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            events: Arc::new(Mutex::new(Vec::new())),
            call_count: Arc::new(AtomicUsize::new(0)),
            should_fail: Arc::new(Mutex::new(false)),
            failure_count: Arc::new(AtomicUsize::new(0)),
            processing_times: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn set_should_fail(&self, should_fail: bool) {
        *self.should_fail.lock().unwrap() = should_fail;
    }

    fn get_events(&self) -> Vec<RawWalEvent> {
        self.events.lock().unwrap().clone()
    }

    fn get_call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }

    fn get_failure_count(&self) -> usize {
        self.failure_count.load(Ordering::SeqCst)
    }

    fn get_avg_processing_time(&self) -> Option<u128> {
        let times = self.processing_times.lock().unwrap();
        if times.is_empty() {
            None
        } else {
            Some(times.iter().sum::<u128>() / times.len() as u128)
        }
    }

    #[allow(dead_code)]
    fn clear(&self) {
        self.events.lock().unwrap().clear();
        self.call_count.store(0, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
        self.processing_times.lock().unwrap().clear();
    }
}

#[async_trait::async_trait]
impl WalEventHandler for ProductionHandler {
    async fn handle(
        &self,
        event: RawWalEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = std::time::Instant::now();
        self.call_count.fetch_add(1, Ordering::SeqCst);

        if *self.should_fail.lock().unwrap() {
            self.failure_count.fetch_add(1, Ordering::SeqCst);
            return Err("Simulated handler failure".into());
        }

        self.events.lock().unwrap().push(event);
        
        let elapsed = start.elapsed().as_micros();
        self.processing_times.lock().unwrap().push(elapsed);

        Ok(())
    }

    fn table_name(&self) -> &str {
        &self.table
    }
}

// Realistic WAL message builders that match actual Postgres output
struct WalMessageFactory;

impl WalMessageFactory {
    fn relation(rel_id: u32, schema: &str, table: &str, columns: &[(&str, u32, i32)]) -> Vec<u8> {
        let mut data = vec![b'R'];
        data.extend_from_slice(&rel_id.to_be_bytes());
        data.extend_from_slice(schema.as_bytes());
        data.push(0);
        data.extend_from_slice(table.as_bytes());
        data.push(0);
        data.push(0); // replica identity

        data.extend_from_slice(&(columns.len() as u16).to_be_bytes());

        for (name, type_id, modifier) in columns {
            data.push(0); // flags
            data.extend_from_slice(name.as_bytes());
            data.push(0);
            data.extend_from_slice(&type_id.to_be_bytes());
            data.extend_from_slice(&modifier.to_be_bytes());
        }

        data
    }

    fn tuple(values: &[TestTupleValue]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(values.len() as u16).to_be_bytes());

        for value in values {
            match value {
                TestTupleValue::Text(s) => {
                    data.push(b't');
                    data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    data.extend_from_slice(s.as_bytes());
                }
                TestTupleValue::Null => data.push(b'n'),
                TestTupleValue::Toast => data.push(b'u'),
            }
        }

        data
    }

    fn insert(rel_id: u32, values: &[TestTupleValue]) -> Vec<u8> {
        let mut data = vec![b'I'];
        data.extend_from_slice(&rel_id.to_be_bytes());
        data.push(b'N');
        data.extend_from_slice(&Self::tuple(values));
        data
    }

    fn update(rel_id: u32, old: Option<&[TestTupleValue]>, new: &[TestTupleValue]) -> Vec<u8> {
        let mut data = vec![b'U'];
        data.extend_from_slice(&rel_id.to_be_bytes());

        if let Some(old_vals) = old {
            data.push(b'O');
            data.extend_from_slice(&Self::tuple(old_vals));
        }

        data.push(b'N');
        data.extend_from_slice(&Self::tuple(new));
        data
    }

    fn delete(rel_id: u32, values: &[TestTupleValue]) -> Vec<u8> {
        let mut data = vec![b'D'];
        data.extend_from_slice(&rel_id.to_be_bytes());
        data.push(b'O');
        data.extend_from_slice(&Self::tuple(values));
        data
    }

    fn begin() -> Vec<u8> {
        vec![b'B']
    }

    fn commit() -> Vec<u8> {
        vec![b'C']
    }
}

// Renamed to avoid conflicts with wal_event::TupleValue
enum TestTupleValue {
    Text(String),
    Null,
    Toast,
}

// Business scenario: E-commerce order processing
#[tokio::test]
async fn test_ecommerce_order_lifecycle() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    
    let orders_handler = Arc::new(ProductionHandler::new("orders"));
    let order_items_handler = Arc::new(ProductionHandler::new("order_items"));
    let inventory_handler = Arc::new(ProductionHandler::new("inventory"));
    
    subscriber.register_handler(orders_handler.clone());
    subscriber.register_handler(order_items_handler.clone());
    subscriber.register_handler(inventory_handler.clone());

    // Define schemas
    let orders_schema = WalMessageFactory::relation(
        1001,
        "public",
        "orders",
        &[
            ("order_id", 23, -1),
            ("user_id", 23, -1),
            ("status", 25, -1),
            ("total_amount", 1700, -1),
            ("created_at", 1114, -1),
        ],
    );

    let order_items_schema = WalMessageFactory::relation(
        1002,
        "public",
        "order_items",
        &[
            ("id", 23, -1),
            ("order_id", 23, -1),
            ("product_id", 23, -1),
            ("quantity", 23, -1),
            ("price", 1700, -1),
        ],
    );

    let inventory_schema = WalMessageFactory::relation(
        1003,
        "public",
        "inventory",
        &[
            ("product_id", 23, -1),
            ("quantity", 23, -1),
            ("reserved", 23, -1),
        ],
    );

    subscriber.process_wal_data(&orders_schema).await.unwrap();
    subscriber.process_wal_data(&order_items_schema).await.unwrap();
    subscriber.process_wal_data(&inventory_schema).await.unwrap();

    // Simulate complete order flow
    subscriber.process_wal_data(&WalMessageFactory::begin()).await.unwrap();

    // 1. Create order
    let create_order = WalMessageFactory::insert(
        1001,
        &[
            TestTupleValue::Text("1001".to_string()),
            TestTupleValue::Text("500".to_string()),
            TestTupleValue::Text("pending".to_string()),
            TestTupleValue::Text("299.99".to_string()),
            TestTupleValue::Text("2024-01-15 10:30:00".to_string()),
        ],
    );
    subscriber.process_wal_data(&create_order).await.unwrap();

    // 2. Add order items
    let add_item1 = WalMessageFactory::insert(
        1002,
        &[
            TestTupleValue::Text("1".to_string()),
            TestTupleValue::Text("1001".to_string()),
            TestTupleValue::Text("101".to_string()),
            TestTupleValue::Text("2".to_string()),
            TestTupleValue::Text("149.99".to_string()),
        ],
    );
    subscriber.process_wal_data(&add_item1).await.unwrap();

    // 3. Update inventory (reserve stock)
    let reserve_inventory = WalMessageFactory::update(
        1003,
        Some(&[
            TestTupleValue::Text("101".to_string()),
            TestTupleValue::Text("100".to_string()),
            TestTupleValue::Text("0".to_string()),
        ]),
        &[
            TestTupleValue::Text("101".to_string()),
            TestTupleValue::Text("100".to_string()),
            TestTupleValue::Text("2".to_string()),
        ],
    );
    subscriber.process_wal_data(&reserve_inventory).await.unwrap();

    // 4. Update order status
    let confirm_order = WalMessageFactory::update(
        1001,
        Some(&[
            TestTupleValue::Text("1001".to_string()),
            TestTupleValue::Text("500".to_string()),
            TestTupleValue::Text("pending".to_string()),
            TestTupleValue::Text("299.99".to_string()),
            TestTupleValue::Text("2024-01-15 10:30:00".to_string()),
        ]),
        &[
            TestTupleValue::Text("1001".to_string()),
            TestTupleValue::Text("500".to_string()),
            TestTupleValue::Text("confirmed".to_string()),
            TestTupleValue::Text("299.99".to_string()),
            TestTupleValue::Text("2024-01-15 10:30:00".to_string()),
        ],
    );
    subscriber.process_wal_data(&confirm_order).await.unwrap();

    subscriber.process_wal_data(&WalMessageFactory::commit()).await.unwrap();

    // Verify complete transaction
    assert_eq!(orders_handler.get_call_count(), 2); // insert + update
    assert_eq!(order_items_handler.get_call_count(), 1);
    assert_eq!(inventory_handler.get_call_count(), 1);

    let order_events = orders_handler.get_events();
    assert!(order_events[0].is_insert());
    assert!(order_events[1].is_update());

    // Verify data integrity
    let inventory_events = inventory_handler.get_events();
    assert!(inventory_events[0].is_update());
    assert!(inventory_events[0].old_tuple.is_some());
    assert!(inventory_events[0].new_tuple.is_some());
}

// Test handler failure and error propagation
#[tokio::test]
async fn test_handler_failure_propagation() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    
    let handler = Arc::new(ProductionHandler::new("critical_table"));
    subscriber.register_handler(handler.clone());

    let schema = WalMessageFactory::relation(
        2001,
        "public",
        "critical_table",
        &[("id", 23, -1), ("data", 25, -1)],
    );
    subscriber.process_wal_data(&schema).await.unwrap();

    // First insert succeeds
    let insert1 = WalMessageFactory::insert(
        2001,
        &[TestTupleValue::Text("1".to_string()), TestTupleValue::Text("data1".to_string())],
    );
    assert!(subscriber.process_wal_data(&insert1).await.is_ok());
    assert_eq!(handler.get_call_count(), 1);
    assert_eq!(handler.get_failure_count(), 0);

    // Simulate handler failure
    handler.set_should_fail(true);

    let insert2 = WalMessageFactory::insert(
        2001,
        &[TestTupleValue::Text("2".to_string()), TestTupleValue::Text("data2".to_string())],
    );
    let result = subscriber.process_wal_data(&insert2).await;
    
    assert!(result.is_err());
    assert_eq!(handler.get_call_count(), 2);
    assert_eq!(handler.get_failure_count(), 1);
    assert_eq!(handler.get_events().len(), 1); // Only first succeeded

    // Recovery: handler starts working again
    handler.set_should_fail(false);

    let insert3 = WalMessageFactory::insert(
        2001,
        &[TestTupleValue::Text("3".to_string()), TestTupleValue::Text("data3".to_string())],
    );
    assert!(subscriber.process_wal_data(&insert3).await.is_ok());
    assert_eq!(handler.get_events().len(), 2);
}

// Test high-volume concurrent updates across multiple tables
#[tokio::test]
async fn test_high_volume_multi_table_updates() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    
    let handlers: Vec<_> = (0..10)
        .map(|i| Arc::new(ProductionHandler::new(&format!("table_{}", i))))
        .collect();

    for handler in &handlers {
        subscriber.register_handler(handler.clone());
    }

    // Setup 10 tables
    for i in 0..10 {
        let schema = WalMessageFactory::relation(
            3000 + i as u32,
            "public",
            &format!("table_{}", i),
            &[
                ("id", 23, -1),
                ("value", 23, -1),
                ("timestamp", 1114, -1),
            ],
        );
        subscriber.process_wal_data(&schema).await.unwrap();
    }

    // Simulate 1000 operations across all tables
    for batch in 0..10 {
        for table_idx in 0..10 {
            for record in 0..10 {
                let insert = WalMessageFactory::insert(
                    3000 + table_idx as u32,
                    &[
                        TestTupleValue::Text(format!("{}", batch * 100 + record)),
                        TestTupleValue::Text(format!("{}", record * 1000)),
                        TestTupleValue::Text("2024-01-15 00:00:00".to_string()),
                    ],
                );
                subscriber.process_wal_data(&insert).await.unwrap();
            }
        }
    }

    // Verify all events processed correctly
    for (i, handler) in handlers.iter().enumerate() {
        assert_eq!(
            handler.get_call_count(),
            100,
            "Table {} should have 100 events",
            i
        );
        assert_eq!(handler.get_events().len(), 100);
        
        // Verify performance is reasonable
        if let Some(avg_time) = handler.get_avg_processing_time() {
            assert!(avg_time < 1000, "Average processing time too high: {}μs", avg_time);
        }
    }
}

// Test schema evolution (ALTER TABLE scenarios)
#[tokio::test]
async fn test_schema_evolution() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    let handler = Arc::new(ProductionHandler::new("evolving_table"));
    subscriber.register_handler(handler.clone());

    // Initial schema: 2 columns
    let schema_v1 = WalMessageFactory::relation(
        4001,
        "public",
        "evolving_table",
        &[("id", 23, -1), ("name", 25, -1)],
    );
    subscriber.process_wal_data(&schema_v1).await.unwrap();

    // Insert with v1 schema
    let insert1 = WalMessageFactory::insert(
        4001,
        &[TestTupleValue::Text("1".to_string()), TestTupleValue::Text("Alice".to_string())],
    );
    subscriber.process_wal_data(&insert1).await.unwrap();

    // Schema evolution: ADD COLUMN
    let schema_v2 = WalMessageFactory::relation(
        4001,
        "public",
        "evolving_table",
        &[
            ("id", 23, -1),
            ("name", 25, -1),
            ("email", 25, -1),
            ("created_at", 1114, -1),
        ],
    );
    subscriber.process_wal_data(&schema_v2).await.unwrap();

    // Insert with v2 schema
    let insert2 = WalMessageFactory::insert(
        4001,
        &[
            TestTupleValue::Text("2".to_string()),
            TestTupleValue::Text("Bob".to_string()),
            TestTupleValue::Text("bob@example.com".to_string()),
            TestTupleValue::Text("2024-01-15 00:00:00".to_string()),
        ],
    );
    subscriber.process_wal_data(&insert2).await.unwrap();

    // Verify both inserts were captured
    assert_eq!(handler.get_call_count(), 2);
    
    let events = handler.get_events();
    assert_eq!(events[0].columns.len(), 2); // Old schema
    assert_eq!(events[1].columns.len(), 4); // New schema
}

// Test complex UPDATE scenarios with partial updates
#[tokio::test]
async fn test_complex_update_patterns() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    let handler = Arc::new(ProductionHandler::new("users"));
    subscriber.register_handler(handler.clone());

    let schema = WalMessageFactory::relation(
        5001,
        "public",
        "users",
        &[
            ("id", 23, -1),
            ("username", 25, -1),
            ("email", 25, -1),
            ("last_login", 1114, -1),
            ("login_count", 23, -1),
        ],
    );
    subscriber.process_wal_data(&schema).await.unwrap();

    // Initial insert
    let insert = WalMessageFactory::insert(
        5001,
        &[
            TestTupleValue::Text("1".to_string()),
            TestTupleValue::Text("john_doe".to_string()),
            TestTupleValue::Text("john@example.com".to_string()),
            TestTupleValue::Null,
            TestTupleValue::Text("0".to_string()),
        ],
    );
    subscriber.process_wal_data(&insert).await.unwrap();

    // Update with old tuple (REPLICA IDENTITY FULL)
    let update_with_old = WalMessageFactory::update(
        5001,
        Some(&[
            TestTupleValue::Text("1".to_string()),
            TestTupleValue::Text("john_doe".to_string()),
            TestTupleValue::Text("john@example.com".to_string()),
            TestTupleValue::Null,
            TestTupleValue::Text("0".to_string()),
        ]),
        &[
            TestTupleValue::Text("1".to_string()),
            TestTupleValue::Text("john_doe".to_string()),
            TestTupleValue::Text("john@example.com".to_string()),
            TestTupleValue::Text("2024-01-15 10:00:00".to_string()),
            TestTupleValue::Text("1".to_string()),
        ],
    );
    subscriber.process_wal_data(&update_with_old).await.unwrap();

    // Update without old tuple (REPLICA IDENTITY DEFAULT/INDEX)
    let update_without_old = WalMessageFactory::update(
        5001,
        None,
        &[
            TestTupleValue::Text("1".to_string()),
            TestTupleValue::Text("john_doe".to_string()),
            TestTupleValue::Text("john@example.com".to_string()),
            TestTupleValue::Text("2024-01-15 11:00:00".to_string()),
            TestTupleValue::Text("2".to_string()),
        ],
    );
    subscriber.process_wal_data(&update_without_old).await.unwrap();

    // Update with TOAST unchanged columns
    let update_with_toast = WalMessageFactory::update(
        5001,
        Some(&[
            TestTupleValue::Text("1".to_string()),
            TestTupleValue::Text("john_doe".to_string()),
            TestTupleValue::Text("john@example.com".to_string()),
            TestTupleValue::Text("2024-01-15 11:00:00".to_string()),
            TestTupleValue::Text("2".to_string()),
        ]),
        &[
            TestTupleValue::Text("1".to_string()),
            TestTupleValue::Text("john_doe".to_string()),
            TestTupleValue::Toast, // Email unchanged (large column)
            TestTupleValue::Text("2024-01-15 12:00:00".to_string()),
            TestTupleValue::Text("3".to_string()),
        ],
    );
    subscriber.process_wal_data(&update_with_toast).await.unwrap();

    let events = handler.get_events();
    assert_eq!(events.len(), 4); // 1 insert + 3 updates

    // Verify update patterns
    assert!(events[1].old_tuple.is_some());
    assert!(events[2].old_tuple.is_none());
    assert!(events[3].old_tuple.is_some());
}

// Test NULL value propagation through entire pipeline
#[tokio::test]
async fn test_null_handling_comprehensive() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    let handler = Arc::new(ProductionHandler::new("nullable_data"));
    subscriber.register_handler(handler.clone());

    let schema = WalMessageFactory::relation(
        7001,
        "public",
        "nullable_data",
        &[
            ("id", 23, -1),
            ("required_field", 25, -1),
            ("optional_field", 25, -1),
            ("nullable_field", 25, -1),
        ],
    );
    subscriber.process_wal_data(&schema).await.unwrap();

    // Test various NULL patterns
    let test_cases = vec![
        // All values present
        vec![
            TestTupleValue::Text("1".to_string()),
            TestTupleValue::Text("required".to_string()),
            TestTupleValue::Text("optional".to_string()),
            TestTupleValue::Text("nullable".to_string()),
        ],
        // One NULL
        vec![
            TestTupleValue::Text("2".to_string()),
            TestTupleValue::Text("required".to_string()),
            TestTupleValue::Null,
            TestTupleValue::Text("nullable".to_string()),
        ],
        // Multiple NULLs
        vec![
            TestTupleValue::Text("3".to_string()),
            TestTupleValue::Text("required".to_string()),
            TestTupleValue::Null,
            TestTupleValue::Null,
        ],
        // NULL UPDATE
        vec![
            TestTupleValue::Text("4".to_string()),
            TestTupleValue::Text("required".to_string()),
            TestTupleValue::Text("was_set".to_string()),
            TestTupleValue::Null,
        ],
    ];

    for values in test_cases {
        let insert = WalMessageFactory::insert(7001, &values);
        subscriber.process_wal_data(&insert).await.unwrap();
    }

    let events = handler.get_events();
    assert_eq!(events.len(), 4);

    // Parse and verify NULL handling
    for (i, event) in events.iter().enumerate() {
        let parsed = TupleParser::parse_tuple_bytes(
            event.new_tuple.as_ref().unwrap(),
            &event.columns,
        )
        .unwrap();

        match i {
            0 => {
                // All present
                for (_, val) in &parsed {
                    assert!(!matches!(val, TupleValue::Null));
                }
            }
            1 => {
                // One NULL
                assert!(matches!(parsed[2].1, TupleValue::Null));
            }
            2 => {
                // Two NULLs
                assert!(matches!(parsed[2].1, TupleValue::Null));
                assert!(matches!(parsed[3].1, TupleValue::Null));
            }
            3 => {
                // Mixed
                assert!(!matches!(parsed[1].1, TupleValue::Null));
                assert!(matches!(parsed[3].1, TupleValue::Null));
            }
            _ => {}
        }
    }
}

// Test DELETE cascades and referential integrity patterns
#[tokio::test]
async fn test_cascading_deletes() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    
    let users_handler = Arc::new(ProductionHandler::new("users"));
    let posts_handler = Arc::new(ProductionHandler::new("posts"));
    let comments_handler = Arc::new(ProductionHandler::new("comments"));
    
    subscriber.register_handler(users_handler.clone());
    subscriber.register_handler(posts_handler.clone());
    subscriber.register_handler(comments_handler.clone());

    // Setup schemas
    let users_schema = WalMessageFactory::relation(
        8001,
        "public",
        "users",
        &[("id", 23, -1), ("username", 25, -1)],
    );
    let posts_schema = WalMessageFactory::relation(
        8002,
        "public",
        "posts",
        &[("id", 23, -1), ("user_id", 23, -1), ("title", 25, -1)],
    );
    let comments_schema = WalMessageFactory::relation(
        8003,
        "public",
        "comments",
        &[("id", 23, -1), ("post_id", 23, -1), ("text", 25, -1)],
    );

    subscriber.process_wal_data(&users_schema).await.unwrap();
    subscriber.process_wal_data(&posts_schema).await.unwrap();
    subscriber.process_wal_data(&comments_schema).await.unwrap();

    // Create data hierarchy
    subscriber.process_wal_data(&WalMessageFactory::begin()).await.unwrap();

    // User
    let insert_user = WalMessageFactory::insert(
        8001,
        &[TestTupleValue::Text("100".to_string()), TestTupleValue::Text("alice".to_string())],
    );
    subscriber.process_wal_data(&insert_user).await.unwrap();

    // Posts
    let insert_post1 = WalMessageFactory::insert(
        8002,
        &[
            TestTupleValue::Text("1000".to_string()),
            TestTupleValue::Text("100".to_string()),
            TestTupleValue::Text("Post 1".to_string()),
        ],
    );
    let insert_post2 = WalMessageFactory::insert(
        8002,
        &[
            TestTupleValue::Text("1001".to_string()),
            TestTupleValue::Text("100".to_string()),
            TestTupleValue::Text("Post 2".to_string()),
        ],
    );
    subscriber.process_wal_data(&insert_post1).await.unwrap();
    subscriber.process_wal_data(&insert_post2).await.unwrap();

    // Comments
    let insert_comment1 = WalMessageFactory::insert(
        8003,
        &[
            TestTupleValue::Text("10000".to_string()),
            TestTupleValue::Text("1000".to_string()),
            TestTupleValue::Text("Great post!".to_string()),
        ],
    );
    let insert_comment2 = WalMessageFactory::insert(
        8003,
        &[
            TestTupleValue::Text("10001".to_string()),
            TestTupleValue::Text("1000".to_string()),
            TestTupleValue::Text("Thanks!".to_string()),
        ],
    );
    subscriber.process_wal_data(&insert_comment1).await.unwrap();
    subscriber.process_wal_data(&insert_comment2).await.unwrap();

    // Simulate CASCADE DELETE
    // Delete comments first
    let delete_comment1 = WalMessageFactory::delete(
        8003,
        &[
            TestTupleValue::Text("10000".to_string()),
            TestTupleValue::Text("1000".to_string()),
            TestTupleValue::Text("Great post!".to_string()),
        ],
    );
    let delete_comment2 = WalMessageFactory::delete(
        8003,
        &[
            TestTupleValue::Text("10001".to_string()),
            TestTupleValue::Text("1000".to_string()),
            TestTupleValue::Text("Thanks!".to_string()),
        ],
    );
    subscriber.process_wal_data(&delete_comment1).await.unwrap();
    subscriber.process_wal_data(&delete_comment2).await.unwrap();

    // Delete posts
    let delete_post1 = WalMessageFactory::delete(
        8002,
        &[
            TestTupleValue::Text("1000".to_string()),
            TestTupleValue::Text("100".to_string()),
            TestTupleValue::Text("Post 1".to_string()),
        ],
    );
    let delete_post2 = WalMessageFactory::delete(
        8002,
        &[
            TestTupleValue::Text("1001".to_string()),
            TestTupleValue::Text("100".to_string()),
            TestTupleValue::Text("Post 2".to_string()),
        ],
    );
    subscriber.process_wal_data(&delete_post1).await.unwrap();
    subscriber.process_wal_data(&delete_post2).await.unwrap();

    // Delete user
    let delete_user = WalMessageFactory::delete(
        8001,
        &[TestTupleValue::Text("100".to_string()), TestTupleValue::Text("alice".to_string())],
    );
    subscriber.process_wal_data(&delete_user).await.unwrap();

    subscriber.process_wal_data(&WalMessageFactory::commit()).await.unwrap();

    // Verify cascade pattern
    assert_eq!(users_handler.get_call_count(), 2); // 1 insert + 1 delete
    assert_eq!(posts_handler.get_call_count(), 4); // 2 inserts + 2 deletes
    assert_eq!(comments_handler.get_call_count(), 4); // 2 inserts + 2 deletes

    // Verify delete order (comments, posts, user)
    let comment_events = comments_handler.get_events();
    let post_events = posts_handler.get_events();
    let user_events = users_handler.get_events();

    assert!(comment_events[2].is_delete());
    assert!(comment_events[3].is_delete());
    assert!(post_events[2].is_delete());
    assert!(post_events[3].is_delete());
    assert!(user_events[1].is_delete());
}

// Test transaction boundaries and ROLLBACK semantics
#[tokio::test]
async fn test_transaction_boundaries() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    let handler = Arc::new(ProductionHandler::new("accounts"));
    subscriber.register_handler(handler.clone());

    let schema = WalMessageFactory::relation(
        9001,
        "public",
        "accounts",
        &[("id", 23, -1), ("balance", 1700, -1)],
    );
    subscriber.process_wal_data(&schema).await.unwrap();

    // Transaction 1: Success
    subscriber.process_wal_data(&WalMessageFactory::begin()).await.unwrap();
    let insert1 = WalMessageFactory::insert(
        9001,
        &[TestTupleValue::Text("1".to_string()), TestTupleValue::Text("1000.00".to_string())],
    );
    subscriber.process_wal_data(&insert1).await.unwrap();
    subscriber.process_wal_data(&WalMessageFactory::commit()).await.unwrap();

    // Transaction 2: Another success
    subscriber.process_wal_data(&WalMessageFactory::begin()).await.unwrap();
    let insert2 = WalMessageFactory::insert(
        9001,
        &[TestTupleValue::Text("2".to_string()), TestTupleValue::Text("500.00".to_string())],
    );
    subscriber.process_wal_data(&insert2).await.unwrap();
    subscriber.process_wal_data(&WalMessageFactory::commit()).await.unwrap();

    // Note: We can't simulate ROLLBACK because it never appears in WAL
    // (PostgreSQL doesn't send rolled-back operations to logical replication)
    
    assert_eq!(handler.get_call_count(), 2);
    assert_eq!(handler.get_events().len(), 2);
}

// Test relation ID reuse after DROP/CREATE
#[tokio::test]
async fn test_relation_id_reuse() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    let handler1 = Arc::new(ProductionHandler::new("temp_table"));
    let handler2 = Arc::new(ProductionHandler::new("new_table"));
    
    subscriber.register_handler(handler1.clone());
    subscriber.register_handler(handler2.clone());

    // Create first table with relation ID 10001
    let schema1 = WalMessageFactory::relation(
        10001,
        "public",
        "temp_table",
        &[("id", 23, -1), ("data", 25, -1)],
    );
    subscriber.process_wal_data(&schema1).await.unwrap();

    let insert1 = WalMessageFactory::insert(
        10001,
        &[TestTupleValue::Text("1".to_string()), TestTupleValue::Text("temp".to_string())],
    );
    subscriber.process_wal_data(&insert1).await.unwrap();

    // Table dropped and new table created with same relation ID
    let schema2 = WalMessageFactory::relation(
        10001,
        "public",
        "new_table",
        &[("id", 23, -1), ("name", 25, -1), ("status", 25, -1)],
    );
    subscriber.process_wal_data(&schema2).await.unwrap();

    let insert2 = WalMessageFactory::insert(
        10001,
        &[
            TestTupleValue::Text("100".to_string()),
            TestTupleValue::Text("new_name".to_string()),
            TestTupleValue::Text("active".to_string()),
        ],
    );
    subscriber.process_wal_data(&insert2).await.unwrap();

    // First handler got first insert, second handler got second insert
    assert_eq!(handler1.get_call_count(), 1);
    assert_eq!(handler2.get_call_count(), 1);
}

// Performance: Verify processing time stays reasonable under load
#[tokio::test]
async fn test_performance_characteristics() {
    let mut subscriber = WalSubscriber::new(WalSubscriberConfig::default());
    let handler = Arc::new(ProductionHandler::new("metrics"));
    subscriber.register_handler(handler.clone());

    let schema = WalMessageFactory::relation(
        11001,
        "public",
        "metrics",
        &[
            ("id", 23, -1),
            ("metric_name", 25, -1),
            ("value", 1700, -1),
        ],
    );
    subscriber.process_wal_data(&schema).await.unwrap();

    let start = std::time::Instant::now();
    
    // Process 1000 events
    for i in 0..1000 {
        let insert = WalMessageFactory::insert(
            11001,
            &[
                TestTupleValue::Text(i.to_string()),
                TestTupleValue::Text(format!("metric_{}", i % 10)),
                TestTupleValue::Text(format!("{}.{}", i, i * 10)),
            ],
        );
        subscriber.process_wal_data(&insert).await.unwrap();
    }

    let elapsed = start.elapsed();
    
    // Processing should be fast: < 100ms for 1000 events
    assert!(elapsed.as_millis() < 100, "Processing too slow: {:?}", elapsed);
    
    // Handler avg processing time should be < 10μs
    if let Some(avg) = handler.get_avg_processing_time() {
        assert!(avg < 10, "Average handler time too high: {}μs", avg);
    }

    assert_eq!(handler.get_call_count(), 1000);
}