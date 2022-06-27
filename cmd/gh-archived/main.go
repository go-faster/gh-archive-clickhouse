package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/mergestat/timediff"
	"go.opentelemetry.io/otel/metric/instrument/asyncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/gh-archive-clickhouse/internal/app"
	"github.com/go-faster/gh-archive-clickhouse/internal/gh"
)

type Service struct {
	clickHouseAddr string
	clickHouseDB   string
	token          string
	lg             *zap.Logger
	batches        chan []gh.Event

	fetchedCount syncint64.Counter
	missCount    syncint64.Counter
	targetRate   asyncfloat64.Gauge
}

func (c *Service) Send(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		db, err := ch.Dial(ctx, ch.Options{
			Database: c.clickHouseDB,
			Address:  c.clickHouseAddr,
		})
		if err != nil {
			return errors.Wrap(err, "dial")
		}

		// Restart stream every softTimeout to force merges.
		softTimeout := time.Now().Add(time.Minute)

		// See table.go for the schema.
		var (
			colID  proto.ColInt64    // id Int64
			colTs  proto.ColDateTime // ts DateTime
			colRaw proto.ColBytes    // raw String
		)
		q := ch.Query{
			Body: "INSERT INTO github_events_raw VALUES",
			Input: proto.Input{
				{Name: "id", Data: &colID},
				{Name: "ts", Data: &colTs},
				{Name: "raw", Data: &colRaw},
			},
			OnInput: func(ctx context.Context) error {
				// Stream events to ClickHouse.
				colID.Reset()
				colTs.Reset()
				colRaw.Reset()
				if time.Now().After(softTimeout) {
					// Restarting stream to force merges.
					return io.EOF
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second * 5):
					// No events for 5 seconds, restarting stream.
					return io.EOF
				case batch := <-c.batches:
					for _, e := range batch {
						colID.Append(e.ID)
						colTs.Append(e.CreatedAt)
						colRaw.Append(e.Raw)
					}
					return nil
				}
			},
		}
		if err := db.Do(ctx, q); err != nil {
			return errors.Wrap(err, "do")
		}
	}
}

func (c *Service) Poll(ctx context.Context) error {
	const (
		perPage  = 100
		maxPages = 10
	)

	client := gh.NewClient(http.DefaultClient, c.token)
	latestMet := make(map[int64]struct{})
	lg := c.lg.Named("poll")

	var etag string
Fetch:
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var rt gh.RateLimit
		var newEvents []gh.Event
		var start time.Time

		currentMet := make(map[int64]struct{})
		for i := 0; i <= maxPages; i++ {
			start = time.Now()
			p := gh.Params{
				Page:    i + 1, // first page is 1
				PerPage: perPage,
			}
			if i == 0 {
				p.Etag = etag
			}
			lg.Info("Fetching events", zap.Int("page", p.Page))
			res, err := client.Events(ctx, p)
			if err != nil {
				return errors.Wrap(err, "failed to fetch events")
			}
			if res.NotModified {
				lg.Info("Not modified", zap.Duration("duration", time.Since(start)))
				continue Fetch
			}
			if res.Unprocessable {
				lg.Warn("Unable to resolve missing events")
				c.missCount.Add(ctx, 1)
				break
			}

			// Updating rate-limit to sleep later.
			rt = res.RateLimit

			// Searching for new events.
			// The currentMet contains events from previous Fetch loop.
			for _, ev := range res.Data {
				if _, ok := currentMet[ev.ID]; ok {
					continue
				}
				currentMet[ev.ID] = struct{}{}
				if _, ok := latestMet[ev.ID]; !ok {
					newEvents = append(newEvents, ev)
				}
			}
			if etag == "" || len(newEvents) < (p.PerPage*p.Page) {
				if i == 0 {
					etag = res.Etag
				}
				break
			}
			// All events are new, fetching next page.
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.batches <- newEvents:
			// Insert events in background.
			c.fetchedCount.Add(ctx, int64(len(newEvents)))
		}

		// Calculating next sleep time to avoid rate limit.
		var targetRate time.Duration
		if rt.Remaining < 10 {
			lg.Warn("Rate limit", zap.Int("remaining", rt.Remaining))
			targetRate = rt.Reset.Sub(time.Now()) + time.Second
		} else {
			targetRate = time.Until(rt.Reset) / time.Duration(rt.Remaining)
		}
		c.targetRate.Observe(ctx, targetRate.Seconds())
		duration := time.Since(start)
		sleep := targetRate - duration
		if sleep <= 0 {
			sleep = 0
		}
		lg.Info("Events",
			zap.Duration("duration", duration),
			zap.Int("new_count", len(newEvents)),
			zap.Int("remaining", rt.Remaining),
			zap.Int("used", rt.Used),
			zap.Duration("reset", rt.Reset.Sub(time.Now())),
			zap.String("reset_human", timediff.TimeDiff(rt.Reset)),
			zap.Duration("sleep", sleep),
			zap.Duration("target_rate", targetRate),
		)
		select {
		case <-time.After(sleep):
			latestMet = currentMet
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger) error {
		g, ctx := errgroup.WithContext(ctx)
		m, err := app.NewMetrics(lg, app.Config{
			Name:      "gh-archived",
			Namespace: "faster",
			Addr:      os.Getenv("METRICS_ADDR"),
		})
		if err != nil {
			return errors.Wrap(err, "failed to create metrics")
		}

		// Initializing metrics.
		meter := m.MeterProvider().Meter("")
		fetchedCount, err := meter.SyncInt64().Counter("events_fetched_count_total")
		if err != nil {
			return errors.Wrap(err, "failed to create counter")
		}
		fetchedCount.Add(ctx, 0) // init
		missCount, err := meter.SyncInt64().Counter("fetch_miss_total")
		if err != nil {
			return errors.Wrap(err, "failed to create counter")
		}
		missCount.Add(ctx, 0) // init
		targetRate, err := meter.AsyncFloat64().Gauge("fetch_target_rate_seconds")
		if err != nil {
			return errors.Wrap(err, "failed to create gauge")
		}

		s := &Service{
			batches:        make(chan []gh.Event, 5),
			lg:             lg,
			clickHouseAddr: os.Getenv("CLICKHOUSE_ADDR"),
			token:          os.Getenv("GITHUB_TOKEN"),
			clickHouseDB:   "faster",

			missCount:    missCount,
			fetchedCount: fetchedCount,
			targetRate:   targetRate,
		}
		g.Go(func() error {
			return s.Poll(ctx)
		})
		g.Go(func() error {
			return s.Send(ctx)
		})
		g.Go(func() error {
			<-ctx.Done()
			return m.Shutdown(context.Background())
		})
		g.Go(func() error {
			return m.Run(ctx)
		})
		return g.Wait()
	})
}
