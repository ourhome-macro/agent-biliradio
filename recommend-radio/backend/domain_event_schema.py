"""Portable domain delivery journal, installed only by versioned migrations."""

STATEMENTS = (
    """CREATE TABLE domain_event_sequences (
        aggregate_key TEXT PRIMARY KEY, sequence INTEGER NOT NULL DEFAULT 0)""",
    """CREATE TABLE domain_event_outbox (
        event_id TEXT PRIMARY KEY, aggregate_key TEXT NOT NULL, sequence INTEGER NOT NULL,
        event_json TEXT NOT NULL, event_hash TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending', available_at REAL NOT NULL DEFAULT 0,
        publish_token TEXT, publish_lease_until REAL NOT NULL DEFAULT 0,
        publish_attempts INTEGER NOT NULL DEFAULT 0, published_at REAL,
        kafka_topic TEXT, kafka_partition INTEGER, kafka_offset INTEGER,
        last_error TEXT, created_at REAL NOT NULL, UNIQUE(aggregate_key,sequence))""",
    "CREATE INDEX idx_domain_outbox_pending "
    "ON domain_event_outbox(status,available_at,publish_lease_until)",
    """CREATE TABLE domain_event_inbox (
        consumer_name TEXT NOT NULL, event_id TEXT NOT NULL, event_hash TEXT NOT NULL,
        aggregate_key TEXT NOT NULL, sequence INTEGER NOT NULL, processed_at REAL NOT NULL,
        PRIMARY KEY(consumer_name,event_id))""",
    """CREATE TABLE domain_consumer_heads (
        consumer_name TEXT NOT NULL, aggregate_key TEXT NOT NULL,
        last_sequence INTEGER NOT NULL, PRIMARY KEY(consumer_name,aggregate_key))""",
    """CREATE TABLE job_worker_leases (
        worker_id TEXT PRIMARY KEY, transport TEXT NOT NULL, lanes_json TEXT NOT NULL,
        lease_until REAL NOT NULL, updated_at REAL NOT NULL)""",
    """CREATE TABLE kafka_delivery_rejections (
        consumer_name TEXT NOT NULL, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
        offset_id INTEGER NOT NULL, payload_hash TEXT NOT NULL, error_type TEXT NOT NULL,
        recorded_at REAL NOT NULL, PRIMARY KEY(consumer_name,topic,partition_id,offset_id))""",
)

JOB_PUBLISH_COLUMNS = (
    ("publish_token", "TEXT"),
    ("publish_lease_until", "REAL NOT NULL DEFAULT 0"),
    ("publish_attempts", "INTEGER NOT NULL DEFAULT 0"),
    ("last_publish_error", "TEXT"),
    ("published_at", "REAL"),
)
