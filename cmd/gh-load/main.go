package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/gzip"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-faster/gh-archive-clickhouse/internal/gh"
)

func run(ctx context.Context, lg *zap.Logger) error {
	var arg struct {
		Key string
	}
	flag.StringVar(&arg.Key, "key", "2022-03-14-21", "key to use")
	flag.Parse()
	db, err := ch.Dial(ctx, ch.Options{
		Database: "faster",
		Address:  os.Getenv("CLICKHOUSE_ADDR"),
	})
	if err != nil {
		return errors.Wrap(err, "dial")
	}

	// https://data.gharchive.org/2015-01-01-15.json.gz
	u := &url.URL{
		Scheme: "https",
		Host:   "data.gharchive.org",
		Path:   arg.Key + ".json.gz",
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 200:
		// OK
	default:
		return errors.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	fmt.Println(resp.Header.Get("Content-Encoding"))

	r, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer func() {
		_ = r.Close()
	}()

	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 1024*1024), 1024*1024)

	var (
		colID  proto.ColInt64    // id Int64
		colTs  proto.ColDateTime // ts DateTime
		colRaw proto.ColBytes    // raw String
	)
	const (
		perBatch = 10_000
	)
	if err := db.Do(ctx, ch.Query{
		Body: "INSERT INTO github_events_raw VALUES",
		Input: proto.Input{
			{Name: "id", Data: &colID},
			{Name: "ts", Data: &colTs},
			{Name: "raw", Data: &colRaw},
		},
		OnInput: func(ctx context.Context) error {
			colID.Reset()
			colTs.Reset()
			colRaw.Reset()

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
			return io.EOF
		},
	}); err != nil {
		return errors.Wrap(err, "do")
	}

	for s.Scan() {
		e := gh.Event{
			Raw: s.Bytes(),
		}
		if err := e.Parse(); err != nil {
			return errors.Wrap(err, "parse")
		}
		lg.Info("Entry",
			zap.Int64("id", e.ID),
		)
	}
	if err := s.Err(); err != nil {
		return err
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
