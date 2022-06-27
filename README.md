# gh-archive-clickhouse
Save public github event stream to ClickHouse as raw json

```sql
CREATE TABLE github_events_raw
(
    id  Int64,
    ts  DateTime32,
    raw String CODEC (ZSTD(16))
) ENGINE = ReplacingMergeTree
      PARTITION BY toYYYYMMDD(ts)
      ORDER BY (ts, id);
```