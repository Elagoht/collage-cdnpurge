// Package cdnpurge is a collage plugin that purges a CDN's copies of the pages
// collage invalidates.
//
//	app, err := collage.New(&collage.Config{
//		Plugins: []collage.Plugin{cdnpurge.New(cdnpurge.Options{
//			BaseURL: "https://example.com",
//			Cloudflare: &cdnpurge.Cloudflare{
//				ZoneID:   os.Getenv("CF_ZONE_ID"),
//				APIToken: os.Getenv("CF_API_TOKEN"),
//			},
//		})},
//	})
//
// When collage drops cached pages it names their paths; the plugin makes them
// absolute with BaseURL and asks the CDN to drop its copies too, so a reader
// behind the CDN sees the new page when a reader of the origin does. Cloudflare is
// purged by URL through its API, and anything else through a webhook that
// receives the URLs as JSON.
//
// Purging is never done by the goroutine that invalidated. What it names is
// collected for a short window — two seconds unless Window says otherwise — so a
// burst of invalidations is one request, and sent in the background, retried with
// backoff when the CDN answers 429 or 5xx. What is still pending when the
// application shuts down is sent before Shutdown returns. A development server
// purges nothing unless Force says so: its invalidations are an editor's, and the
// CDN is production's.
package cdnpurge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Elagoht/collage/pkg/collage"
)

// Name is the plugin's name, and the key its configuration is found under.
const Name = "elagoht/cdnpurge"

// Options configures the plugin.
type Options struct {
	// BaseURL is the public origin the CDN serves the site at,
	// "https://example.com": collage names paths, and a CDN purges URLs.
	// Required.
	BaseURL string `json:"baseURL"`
	// Cloudflare purges a Cloudflare zone. At least one of Cloudflare and
	// Webhook is required; with both, both are purged.
	Cloudflare *Cloudflare `json:"cloudflare"`
	// Webhook posts the URLs to an endpoint of your own, or of a CDN whose API
	// takes a list of URLs.
	Webhook *Webhook `json:"webhook"`
	// Window is how long invalidations are collected before they are sent, so a
	// burst of them is one request. Default "2s".
	Window Duration `json:"window"`
	// Retries is how many times a request the CDN answered 429 or 5xx, or that
	// did not reach it, is tried again. Default 4; negative tries once.
	Retries int `json:"retries"`
	// Backoff is the wait before the first retry, doubled before each one after
	// it. A 429's Retry-After, when longer, is honoured instead. Default "1s".
	Backoff Duration `json:"backoff"`
	// Timeout bounds one request to the CDN. Default "10s".
	Timeout Duration `json:"timeout"`
	// Force purges from a development server too, which otherwise purges
	// nothing.
	Force bool `json:"force"`
	// Client sends the requests. Default: an http.Client with no timeout of its
	// own, since each request is bounded by Timeout.
	Client *http.Client `json:"-"`
}

// Cloudflare purges files from one Cloudflare zone.
type Cloudflare struct {
	// ZoneID is the zone's identifier, on the zone's overview page. Required.
	ZoneID string `json:"zoneID"`
	// APIToken is an API token with the Zone → Cache Purge permission for the
	// zone. Required. Keep it out of a configuration file under version control.
	APIToken string `json:"apiToken"`
	// BatchSize is how many URLs one purge request carries. Default 30, which is
	// what every Cloudflare plan accepts; an Enterprise zone takes more.
	BatchSize int `json:"batchSize"`
	// Endpoint is the API's base URL. Default "https://api.cloudflare.com".
	Endpoint string `json:"endpoint"`
}

// Webhook posts {"urls": [...]} to URL.
type Webhook struct {
	// URL receives the POST. Required.
	URL string `json:"url"`
	// Token, when set, is sent as "Authorization: Bearer <token>".
	Token string `json:"token"`
	// BatchSize is how many URLs one request carries. Default: all of them.
	BatchSize int `json:"batchSize"`
}

// Duration is a time.Duration that reads from configuration as Go writes one,
// "2s" or "500ms", as well as a number of nanoseconds.
type Duration time.Duration

// UnmarshalJSON reads "2s" or a number of nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("cdnpurge: %w", err)
		}
		*d = Duration(v)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("cdnpurge: a duration is a string like \"2s\" or a number of nanoseconds")
	}
	*d = Duration(n)
	return nil
}

// ErrNoBaseURL is returned by Init when BaseURL is not an absolute http(s) URL.
var ErrNoBaseURL = errors.New("cdnpurge: BaseURL is required: an absolute URL such as https://example.com")

// ErrNoProvider is returned by Init when neither Cloudflare nor Webhook is set.
var ErrNoProvider = errors.New("cdnpurge: nothing to purge: set Cloudflare or Webhook")

// Plugin purges the CDN.
type Plugin struct {
	opts    Options
	base    string
	logger  *slog.Logger
	targets []target
	enabled bool

	mu      sync.Mutex
	pending map[string]struct{}
	timer   *time.Timer
	closed  bool

	// ctx ends the retries still waiting when Shutdown runs out of time.
	ctx    context.Context
	cancel context.CancelFunc
	sends  sync.WaitGroup
}

// target is one provider: how it turns a batch of URLs into a request.
type target struct {
	name  string
	batch int
	build func(ctx context.Context, urls []string) (*http.Request, error)
	// check reads a successful status's body for a failure it reports anyway.
	check func(body []byte) error
}

// New returns a plugin with opts as its starting point, which the application's
// own configuration is then decoded over.
func New(opts Options) *Plugin { return &Plugin{opts: opts} }

func (p *Plugin) Name() string    { return Name }
func (p *Plugin) Version() string { return "0.1.0" }

var _ collage.CacheInvalidateHook = (*Plugin)(nil)

// Init reads and checks the configuration. A plugin that cannot purge refuses to
// start rather than letting the CDN serve stale pages without a word.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	if err := host.Config(&p.opts); err != nil {
		return err
	}
	p.logger = host.Logger()
	o := &p.opts
	base, err := url.Parse(o.BaseURL)
	if o.BaseURL == "" || err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return ErrNoBaseURL
	}
	p.base = strings.TrimSuffix(o.BaseURL, "/")
	if o.Cloudflare == nil && o.Webhook == nil {
		return ErrNoProvider
	}
	if o.Window <= 0 {
		o.Window = Duration(2 * time.Second)
	}
	if o.Retries == 0 {
		o.Retries = 4
	}
	if o.Backoff <= 0 {
		o.Backoff = Duration(time.Second)
	}
	if o.Timeout <= 0 {
		o.Timeout = Duration(10 * time.Second)
	}
	if o.Client == nil {
		o.Client = &http.Client{}
	}
	p.targets = nil
	if cf := o.Cloudflare; cf != nil {
		t, err := cloudflare(*cf)
		if err != nil {
			return err
		}
		p.targets = append(p.targets, t)
	}
	if wh := o.Webhook; wh != nil {
		t, err := webhook(*wh)
		if err != nil {
			return err
		}
		p.targets = append(p.targets, t)
	}
	p.enabled = !host.DevMode() || o.Force
	p.pending = make(map[string]struct{})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return nil
}

func cloudflare(cf Cloudflare) (target, error) {
	if cf.ZoneID == "" || cf.APIToken == "" {
		return target{}, errors.New("cdnpurge: Cloudflare needs both ZoneID and APIToken")
	}
	if strings.ContainsAny(cf.ZoneID, "/?#") {
		return target{}, fmt.Errorf("cdnpurge: Cloudflare ZoneID %q is not a zone identifier", cf.ZoneID)
	}
	endpoint := strings.TrimSuffix(cf.Endpoint, "/")
	if endpoint == "" {
		endpoint = "https://api.cloudflare.com"
	}
	if err := checkURL(endpoint); err != nil {
		return target{}, fmt.Errorf("cdnpurge: Cloudflare Endpoint: %w", err)
	}
	batch := cf.BatchSize
	if batch <= 0 {
		batch = 30
	}
	purge := endpoint + "/client/v4/zones/" + url.PathEscape(cf.ZoneID) + "/purge_cache"
	return target{
		name:  "cloudflare",
		batch: batch,
		build: func(ctx context.Context, urls []string) (*http.Request, error) {
			body, err := json.Marshal(struct {
				Files []string `json:"files"`
			}{urls})
			if err != nil {
				return nil, err
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, purge, bytes.NewReader(body))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+cf.APIToken)
			return req, nil
		},
		check: func(body []byte) error {
			// Cloudflare answers some refusals with 200 and success: false.
			var answer struct {
				Success bool `json:"success"`
				Errors  []struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				} `json:"errors"`
			}
			if err := json.Unmarshal(body, &answer); err != nil {
				return fmt.Errorf("unreadable answer: %w", err)
			}
			if !answer.Success {
				msgs := make([]string, 0, len(answer.Errors))
				for _, e := range answer.Errors {
					msgs = append(msgs, fmt.Sprintf("%d %s", e.Code, e.Message))
				}
				return fmt.Errorf("refused: %s", strings.Join(msgs, "; "))
			}
			return nil
		},
	}, nil
}

func webhook(wh Webhook) (target, error) {
	if err := checkURL(wh.URL); err != nil {
		return target{}, fmt.Errorf("cdnpurge: Webhook URL: %w", err)
	}
	return target{
		name:  "webhook",
		batch: wh.BatchSize,
		build: func(ctx context.Context, urls []string) (*http.Request, error) {
			body, err := json.Marshal(struct {
				URLs []string `json:"urls"`
			}{urls})
			if err != nil {
				return nil, err
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, wh.URL, bytes.NewReader(body))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/json")
			if wh.Token != "" {
				req.Header.Set("Authorization", "Bearer "+wh.Token)
			}
			return req, nil
		},
	}, nil
}

func checkURL(raw string) error {
	u, err := url.Parse(raw)
	if raw == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q is not an absolute http(s) URL", raw)
	}
	return nil
}

// OnCacheInvalidate queues the dropped paths and returns. The invalidating
// goroutine is often a request's — an action that saved a post — and a CDN's API
// is not something it should wait on.
func (p *Plugin) OnCacheInvalidate(_ context.Context, ev *collage.CacheInvalidateEvent) error {
	if !p.enabled || len(ev.Paths) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	for _, path := range ev.Paths {
		p.pending[p.absolute(path)] = struct{}{}
	}
	if p.timer == nil {
		p.timer = time.AfterFunc(time.Duration(p.opts.Window), p.flush)
	}
	return nil
}

// absolute is path under BaseURL, escaped as a URL path: collage names paths as
// the router matched them, decoded.
func (p *Plugin) absolute(path string) string {
	return p.base + (&url.URL{Path: path}).EscapedPath()
}

// flush sends what is pending, in the background. The timer calls it; once
// Shutdown has begun it has already taken everything, and flush does nothing.
func (p *Plugin) flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.dispatch()
}

// dispatch empties the pending set and starts sending it. The caller holds p.mu,
// which is what orders every sends.Add before Shutdown's sends.Wait.
func (p *Plugin) dispatch() {
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	if len(p.pending) == 0 {
		return
	}
	urls := make([]string, 0, len(p.pending))
	for u := range p.pending {
		urls = append(urls, u)
	}
	clear(p.pending)
	// Sorted, so a batch is the same batch whichever order the invalidations
	// came in.
	slices.Sort(urls)
	p.sends.Add(1)
	go func() {
		defer p.sends.Done()
		p.send(p.ctx, urls)
	}()
}

// send purges urls at every target, in batches, logging what fails.
func (p *Plugin) send(ctx context.Context, urls []string) {
	for _, t := range p.targets {
		size := t.batch
		if size <= 0 {
			size = len(urls)
		}
		for batch := range slices.Chunk(urls, size) {
			if err := p.purge(ctx, t, batch); err != nil {
				p.logger.Error("cdnpurge: purge failed", "provider", t.name, "urls", len(batch), "err", err)
			}
		}
	}
}

// purge sends one batch, retrying what may succeed later: a 429, a 5xx, a
// request that never got an answer. A 4xx other than 429 is a request the CDN
// will refuse however often it is made.
func (p *Plugin) purge(ctx context.Context, t target, urls []string) error {
	wait := time.Duration(p.opts.Backoff)
	for attempt := 0; ; attempt++ {
		retry, after, err := p.attempt(ctx, t, urls)
		if err == nil {
			return nil
		}
		if !retry || attempt >= p.opts.Retries {
			return err
		}
		delay := max(wait, after)
		p.logger.Warn("cdnpurge: retrying", "provider", t.name, "attempt", attempt+1, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (gave up at shutdown)", err)
		case <-time.After(delay):
		}
		wait *= 2
	}
}

// attempt makes one request. It reports whether a failure is worth retrying, and
// how long the CDN asked to be left alone.
func (p *Plugin) attempt(ctx context.Context, t target, urls []string) (retry bool, after time.Duration, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(p.opts.Timeout))
	defer cancel()
	req, err := t.build(ctx, urls)
	if err != nil {
		return false, 0, err
	}
	res, err := p.opts.Client.Do(req)
	if err != nil {
		return true, 0, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	switch {
	case res.StatusCode == http.StatusTooManyRequests:
		return true, retryAfter(res.Header.Get("Retry-After")), fmt.Errorf("%s: %s", res.Status, snippet(body))
	case res.StatusCode >= 500:
		return true, 0, fmt.Errorf("%s: %s", res.Status, snippet(body))
	case res.StatusCode >= 300:
		return false, 0, fmt.Errorf("%s: %s", res.Status, snippet(body))
	}
	if t.check != nil {
		if err := t.check(body); err != nil {
			return false, 0, err
		}
	}
	return false, 0, nil
}

// retryAfter reads Retry-After's seconds form; the date form is rare from an
// API, and missing it only means the plugin's own backoff is used.
func retryAfter(v string) time.Duration {
	if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return 0
}

func snippet(body []byte) string {
	s := strings.TrimSpace(string(body))
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// Shutdown sends what is still pending and waits for every purge in flight, until
// ctx ends; a retry still waiting then gives up and is logged.
func (p *Plugin) Shutdown(ctx context.Context) error {
	if p.cancel == nil {
		return nil // Init never ran
	}
	p.mu.Lock()
	p.closed = true
	p.dispatch()
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		p.sends.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		<-done
		return fmt.Errorf("cdnpurge: purges still in flight at shutdown: %w", ctx.Err())
	}
}
