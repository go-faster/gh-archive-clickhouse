package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/gzip"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/gh-archive-clickhouse/internal/gh"
)

type Application struct {
	lg        *zap.Logger
	tasks     chan time.Time
	list      bool
	buf       int
	batchSize int

	addr  string
	db    string
	table string

	bytesSent atomic.Uint64
	rowsSent  atomic.Uint64
	lastKey   atomic.String
}

func (a *Application) Process(ctx context.Context, t time.Time) error {
	// https://data.gharchive.org/2015-01-01-15.json.gz
	start := time.Now()
	key := fmt.Sprintf("%d-%02d-%02d-%d", t.Year(), t.Month(), t.Day(), t.Hour())
	lg := a.lg.With(zap.String("key", key))
	u := &url.URL{
		Scheme: "https",
		Host:   "data.gharchive.org",
		Path:   key + ".json.gz",
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	switch resp.StatusCode {
	case 200:
		lg.Debug("Found", zap.String("key", key))
		if a.list {
			return nil
		}
	case 404:
		lg.Error("Not found", zap.String("key", key))
		return nil
	default:
		return errors.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	r, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}

	db, err := ch.Dial(ctx, ch.Options{
		Database: a.db,
		Logger:   lg,
		Address:  a.addr,
	})
	if err != nil {
		return errors.Wrap(err, "dial")
	}
	defer func() {
		_ = db.Close()
	}()

	s := bufio.NewScanner(r)
	buf := make([]byte, a.buf)
	s.Buffer(buf, cap(buf))

	var (
		colID  proto.ColInt64    // id Int64
		colTs  proto.ColDateTime // ts DateTime
		colRaw proto.ColBytes    // raw String
	)
	if err := db.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s VALUES", a.table),
		Input: proto.Input{
			{Name: "id", Data: &colID},
			{Name: "ts", Data: &colTs},
			{Name: "raw", Data: &colRaw},
		},
		OnProfile: func(ctx context.Context, p proto.Profile) error {
			lg.Info("Profile",
				zap.Int("rows", int(p.Rows)),
				zap.Int("bytes", int(p.Bytes)),
			)
			return nil
		},
		OnInput: func(ctx context.Context) error {
			colID.Reset()
			colTs.Reset()
			colRaw.Reset()

			lg.Debug("Creating batch")
			defer func() {
				lg.Debug("Batch sent")
			}()

			for s.Scan() {
				e := gh.Event{Raw: s.Bytes()}
				if err := e.Parse(); err != nil {
					return errors.Wrap(err, "parse")
				}

				colID.Append(e.ID)
				colTs.Append(e.CreatedAt)
				colRaw.Append(e.Raw)

				a.bytesSent.Add(uint64(len(e.Raw)))
				a.rowsSent.Add(1)

				if colID.Rows() > a.batchSize {
					// New batch.
					return nil
				}
			}
			if err := s.Err(); err != nil {
				return errors.Wrap(err, "scan")
			}

			return io.EOF
		},
	}); err != nil {
		return errors.Wrap(err, "do")
	}

	a.lastKey.Store(key)
	lg.Debug("Done",
		zap.Duration("duration", time.Since(start)),
	)

	return nil
}

func run(ctx context.Context) error {
	var arg struct {
		From      string
		To        string
		Jobs      int
		Host      string
		Port      int
		DB        string
		Table     string
		Buf       int
		BatchSize int
		LogLevel  string
		Backoff   backoff.ExponentialBackOff
	}

	flag.StringVar(&arg.From, "from", "2015-01-01T00", "start from time (uses Events API format from 2015)")
	flag.StringVar(&arg.To, "to", time.Now().Add(-time.Hour*1).UTC().Format("2006-01-02T15"), "end time (current time minus 1 hour by default)")
	flag.IntVar(&arg.Jobs, "jobs", 1, "jobs")
	flag.IntVar(&arg.Buf, "b", 1024*1024*100, "maximum line size in bytes, will be buffered in memory for each job")
	flag.StringVar(&arg.Host, "host", "localhost", "host")
	flag.IntVar(&arg.Port, "port", 9000, "port")
	flag.StringVar(&arg.DB, "db", "", "db")
	flag.StringVar(&arg.Table, "table", "github_events_raw", "table")
	flag.StringVar(&arg.LogLevel, "log", "info", "log level (debug, info, warn, error, panic, fatal)")
	flag.IntVar(&arg.BatchSize, "batch-size", 10_000, "events in single batch per job")

	flag.DurationVar(&arg.Backoff.MaxInterval, "backoff.max-interval", backoff.DefaultMaxInterval, "backoff max interval")
	flag.DurationVar(&arg.Backoff.MaxElapsedTime, "backoff.max-elapsed-time", backoff.DefaultMaxElapsedTime, "backoff max elapsed time")
	flag.DurationVar(&arg.Backoff.InitialInterval, "backoff.initial-interval", backoff.DefaultInitialInterval, "backoff initial interval")
	flag.Float64Var(&arg.Backoff.Multiplier, "backoff.multiplier", backoff.DefaultMultiplier, "backoff multiplier")
	flag.Float64Var(&arg.Backoff.RandomizationFactor, "backoff.randomization-factor", backoff.DefaultRandomizationFactor, "backoff randomization factor")

	flag.Parse()

	cfg := zap.NewDevelopmentConfig()
	if err := cfg.Level.UnmarshalText([]byte(arg.LogLevel)); err != nil {
		return errors.Wrap(err, "log level")
	}
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	lg, err := cfg.Build()
	if err != nil {
		return errors.Wrap(err, "build logger config")
	}

	start, err := time.Parse("2006-01-02T15", arg.From)
	if err != nil {
		return errors.Wrap(err, "parse start")
	}
	end, err := time.Parse("2006-01-02T15", arg.To)
	if err != nil {
		return errors.Wrap(err, "parse end")
	}

	g, ctx := errgroup.WithContext(ctx)
	a := &Application{
		lg:        lg,
		tasks:     make(chan time.Time, arg.Jobs),
		buf:       arg.Buf,
		batchSize: arg.BatchSize,
		addr:      net.JoinHostPort(arg.Host, strconv.Itoa(arg.Port)),
		db:        arg.DB,
		table:     arg.Table,
	}
	lg.Info("Start",
		zap.String("from", arg.From), zap.String("to", arg.To),
		zap.String("addr", a.addr),
		zap.String("db", a.db),
		zap.String("table", a.table),
	)
	for i := 0; i < arg.Jobs; i++ {
		g.Go(func() error {
			for {
				select {
				case t := <-a.tasks:
					b := backoff.NewExponentialBackOff()
					b.MaxInterval = arg.Backoff.MaxInterval
					b.MaxElapsedTime = arg.Backoff.MaxElapsedTime
					b.InitialInterval = arg.Backoff.InitialInterval
					b.Multiplier = arg.Backoff.Multiplier
					b.RandomizationFactor = arg.Backoff.RandomizationFactor

					if err := backoff.Retry(func() error {
						return a.Process(ctx, t)
					},
						backoff.WithContext(backoff.WithMaxRetries(b, 5), ctx),
					); err != nil {
						return errors.Wrap(err, "process")
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}
	g.Go(func() error {
		rate := time.Second * 3
		ticker := time.NewTicker(rate)
		var (
			lastBytes uint64
			lastRows  uint64
		)
		for {
			select {
			case <-ticker.C:
				var (
					curBytes = a.bytesSent.Load()
					curRows  = a.rowsSent.Load()
				)
				lg.Info("Running",
					zap.String("latest_key", a.lastKey.Load()),
					zap.Uint64("bytes_sent", curBytes),
					zap.String("bytes_sent_human", humanize.Bytes(curBytes)),
					zap.String("bytes_rate", humanize.Bytes(uint64(float64(curBytes-lastBytes)/rate.Seconds()))+"/s"),
					zap.Uint64("rows_sent", curRows),
					zap.String("rows_rate", humanize.Comma(int64(float64(curRows-lastRows)/rate.Seconds()))+"/s"),
				)
				lastBytes = curBytes
				lastRows = curRows
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	g.Go(func() error {
		defer close(a.tasks)
		for t := start; t.Before(end); t = t.Add(time.Hour) {
			select {
			case a.tasks <- t:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "wait")
	}

	return nil
}

func main() {
	ctx := context.Background()

	if err := run(ctx); err != nil {
		panic(err)
	}
}
