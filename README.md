# gh-archive-clickhouse
Save public github event stream to ClickHouse as raw json.

* [List public events](https://docs.github.com/en/rest/activity/events#list-public-events) GitHub endpoint
* Original [GitHub Archive](https://github.com/igrigorik/gharchive.org) project
* [ClickHouse](https://clickhouse.tech/) OLAP database

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

Alternative to [gharchive crawler](https://github.com/igrigorik/gharchive.org/tree/master/crawler) with
decreased probability to miss events.

* Streaming to ClickHouse via native protocol instead of using files, so storage and fethching are decoupled.
* Automatic pagination if more than one page of new events is available
* Automatic fetch rate adjustment based on rate limit github headers and request duration
* ETag support to skip cached results
