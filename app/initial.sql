CREATE DATABASE IF NOT EXISTS logging;

CREATE TABLE IF NOT EXISTS logging.app_logs
(
    timestamp    DateTime64(3),
    level        String,
    service_name String,
    trace_id     String,
    message      String,
    details      Map(String, String)
)
    ENGINE = MergeTree()
        ORDER BY (level, service_name, timestamp)
        TTL timestamp + INTERVAL 30 DAY;