# gh-archive-clickhouse
Save public GitHub event stream to ClickHouse as json.

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

* Streaming to ClickHouse via native protocol instead of using files, so storage and fetching are decoupled.
* Automatic pagination if more than one page of new events is available
* Automatic fetch rate adjustment based on rate limit GitHub headers and request duration
* ETag support to skip cached results

## gh-load

```console
$ gh-load --help
Usage of gh-load:
  -b int
    	max token bytes (default 104857600)
  -db string
    	db
  -from string
    	start from (default "2022-03-14T21")
  -host string
    	host (default "localhost")
  -jobs int
    	jobs (default 1)
  -port int
    	port (default 9000)
  -table string
    	table (default "github_events_raw")
  -to string
    	end with (default "2022-04-14T21")
```

Example usage (consumes ~340MB RAM per job):
```console
gh-load --db faster --from 2020-01-01T00 --to 2020-01-20T00 --jobs 20
```