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
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/gzip"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/gh-archive-clickhouse/internal/gh"
)

type Application struct {
	lg    *zap.Logger
	tasks chan time.Time
	list  bool
	buf   int

	addr  string
	db    string
	table string
}

func (a *Application) Process(ctx context.Context, t time.Time) error {
	// https://data.gharchive.org/2015-01-01-15.json.gz
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
		lg.Info("Found", zap.String("key", key))
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
	const (
		perBatch = 10_000
	)
	if err := db.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s VALUES", a.table),
		Input: proto.Input{
			{Name: "id", Data: &colID},
			{Name: "ts", Data: &colTs},
			{Name: "raw", Data: &colRaw},
		},
		OnProfile: func(ctx context.Context, p proto.Profile) error {
			lg.Info("Profile", zap.Int("rows", int(p.Rows)), zap.Int("bytes", int(p.Bytes)))
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

				if colID.Rows() > perBatch {
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

	lg.Info("Done")

	return nil
}

func run(ctx context.Context, lg *zap.Logger) error {
	var arg struct {
		From  string
		To    string
		Jobs  int
		Host  string
		Port  int
		DB    string
		Table string
		Buf   int
	}
	flag.StringVar(&arg.From, "from", "2022-03-14T21", "start from")
	flag.StringVar(&arg.To, "to", "2022-04-14T21", "end with")
	flag.IntVar(&arg.Jobs, "jobs", 1, "jobs")
	flag.IntVar(&arg.Buf, "b", 1024*1024*100, "max token bytes")
	flag.StringVar(&arg.Host, "host", "localhost", "host")
	flag.IntVar(&arg.Port, "port", 9000, "port")
	flag.StringVar(&arg.DB, "db", "", "db")
	flag.StringVar(&arg.Table, "table", "github_events_raw", "table")
	flag.Parse()

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
		lg:    lg,
		tasks: make(chan time.Time, arg.Jobs),
		buf:   arg.Buf,
		addr:  net.JoinHostPort(arg.Host, strconv.Itoa(arg.Port)),
		db:    arg.DB,
		table: arg.Table,
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
					b := backoff.NewConstantBackOff(time.Second)
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
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	lg, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	if err := run(ctx, lg); err != nil {
		panic(err)
	}
}
