// Package gh is GitHub client.
package gh

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Client struct {
	http   HTTPClient
	rand   *rand.Rand
	tokens []string
}

func NewClient(http HTTPClient, tok string) *Client {
	var tokens []string
	for _, v := range strings.Split(tok, ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		tokens = append(tokens, v)
	}
	return &Client{
		http:   http,
		tokens: tokens,
		rand:   rand.New(rand.NewSource(time.Now().Unix())),
	}
}

type Params struct {
	Etag    string
	PerPage int
	Page    int
}

type Result[T any] struct {
	Data T

	Etag          string
	NotModified   bool
	Unprocessable bool
	RateLimit     RateLimit
}

type RateLimit struct {
	Limit     int
	Remaining int
	Reset     time.Time
	Used      int
}

func (r *RateLimit) Parse(v http.Header) error {
	var err error

	if r.Limit, err = strconv.Atoi(v.Get("X-RateLimit-Limit")); err != nil {
		return errors.Wrap(err, "limit")
	}
	if r.Remaining, err = strconv.Atoi(v.Get("X-RateLimit-Remaining")); err != nil {
		return errors.Wrap(err, "remaining")
	}
	if r.Used, err = strconv.Atoi(v.Get("X-RateLimit-Used")); err != nil {
		return errors.Wrap(err, "used")
	}
	resetUnix, err := strconv.ParseInt(v.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil {
		return errors.Wrap(err, "reset")
	}
	r.Reset = time.Unix(resetUnix, 0)

	return nil
}

type Event struct {
	Raw       jx.Raw
	CreatedAt time.Time
	ID        int64
}

func (e *Event) Parse() error {
	if err := jx.DecodeBytes(e.Raw).ObjBytes(func(d *jx.Decoder, k []byte) error {
		switch string(k) {
		case "created_at":
			v, err := d.Str()
			if err != nil {
				return errors.Wrap(err, "str")
			}
			if e.CreatedAt, err = time.Parse(time.RFC3339, v); err != nil {
				return err
			}
			return nil
		case "id":
			v, err := d.Num()
			if err != nil {
				return errors.Wrap(err, "id")
			}
			id, err := v.Int64()
			if err != nil {
				return err
			}
			e.ID = id
			return nil
		default:
			if err := d.Skip(); err != nil {
				return errors.Wrap(err, "skip")
			}
			return nil
		}
	}); err != nil {
		return err
	}
	return nil
}

func (c *Client) Events(ctx context.Context, p Params) (*Result[[]Event], error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/events", nil)
	if err != nil {
		return nil, err
	}
	if len(c.tokens) > 0 {
		req.Header.Add("Authorization", "token "+c.tokens[c.rand.Intn(len(c.tokens))])
	}
	if p.Etag != "" {
		req.Header.Add("If-None-Match", p.Etag)
	}
	req.Header.Add("User-Agent", "https://github.com/go-faster/gha")
	req.Header.Add("Accept", "application/vnd.github.v3+json")

	q := req.URL.Query()
	if p.PerPage > 0 {
		q.Set("per_page", fmt.Sprintf("%d", p.PerPage))
	}
	if p.Page > 0 {
		q.Set("page", fmt.Sprintf("%d", p.Page))
	}
	req.URL.RawQuery = q.Encode()
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		// ok
	case http.StatusNotModified:
		return &Result[[]Event]{
			NotModified: true,
		}, nil
	case http.StatusUnprocessableEntity:
		return &Result[[]Event]{
			Unprocessable: true,
		}, nil
	default:
		return nil, errors.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	/*
		Headers example:
			Cache-Control: public, max-age=60, s-maxage=60
			Content-Security-Policy: default-src 'none'
			Content-Type: application/json; charset=utf-8
			Date: Sun, 26 Jun 2022 19:48:08 GMT
			Etag: W/"9d7d7dda865701d9fb4ecd7f6d767ecb1b813b086440bc62bb626de5b877dc49"
			Last-Modified: Sun, 26 Jun 2022 19:43:08 GMT
			Link: <https://api.github.com/events?page=2>; rel="next", <https://api.github.com/events?page=10>; rel="last"
			Referrer-Policy: origin-when-cross-origin, strict-origin-when-cross-origin
			Server: GitHub.com
			Strict-Transport-Security: max-age=31536000; includeSubdomains; preload
			Vary: Accept, Accept-Encoding, Accept, X-Requested-With
			X-Content-Type-Options: nosniff
			X-Frame-Options: deny
			X-Github-Media-Type: github.v3; format=json
			X-Github-Request-Id: 8AD2:5A65:2D401F:2EFBE6:62B8B7F7
			X-Poll-Interval: 60
			X-Ratelimit-Limit: 60
			X-Ratelimit-Remaining: 48
			X-Ratelimit-Reset: 1656274556
			X-Ratelimit-Resource: core
			X-Ratelimit-Used: 12
			X-Xss-Protection: 0
	*/

	var events []Event
	if err := jx.Decode(resp.Body, 1024).Arr(func(d *jx.Decoder) error {
		raw, err := d.RawAppend(nil)
		if err != nil {
			return errors.Wrap(err, "extract raw event")
		}
		ev := Event{Raw: raw}
		if err := ev.Parse(); err != nil {
			return errors.Wrap(err, "parse event")
		}
		events = append(events, ev)
		return nil
	}); err != nil {
		return nil, err
	}

	var rateLimit RateLimit
	if err := rateLimit.Parse(resp.Header); err != nil {
		return nil, err
	}

	return &Result[[]Event]{
		Data:      events,
		Etag:      resp.Header.Get("etag"),
		RateLimit: rateLimit,
	}, nil
}
