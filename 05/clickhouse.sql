CREATE TABLE IF NOT EXISTS events (
    event_id UUID, user_id String, movie_id String, event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'), device_type LowCardinality(String),
    session_id String, progress_seconds UInt32
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), user_id, event_id);

CREATE TABLE IF NOT EXISTS incoming (
    event_id String, user_id String, movie_id String,
    event_type Enum8('VIEW_STARTED'=0, 'VIEW_FINISHED'=1, 'VIEW_PAUSED'=2,
                     'VIEW_RESUMED'=3, 'LIKED'=4, 'SEARCHED'=5),
    timestamp Int64, device_type Enum8('MOBILE'=0, 'DESKTOP'=1, 'TV'=2, 'TABLET'=3),
    session_id String, progress_seconds UInt32
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
         kafka_topic_list = 'movie-events', kafka_group_name = 'cinema-clickhouse',
         kafka_format = 'ProtobufSingle', kafka_schema = 'movie.proto:MovieEvent',
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ingest TO events AS
SELECT toUUID(event_id) AS event_id, user_id, movie_id, toString(event_type) AS event_type,
       fromUnixTimestamp64Milli(timestamp, 'UTC') AS timestamp,
       toString(device_type) AS device_type, session_id, progress_seconds
FROM incoming;

CREATE TABLE IF NOT EXISTS daily (
    day Date, dau UInt64, avg_seconds Nullable(Float64), conversion Nullable(Float64),
    top_movies Array(Tuple(String, UInt64)), retention Array(Nullable(Float64)),
    calculated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(calculated_at) ORDER BY day;
