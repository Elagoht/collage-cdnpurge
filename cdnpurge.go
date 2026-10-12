// Package cdnpurge is a collage plugin that purges a CDN's copies of the pages
// collage invalidates.
//
//	app, err := collage.New(&collage.Config{
//		BaseURL: "https://example.com",
//		Plugins: []collage.Plugin{cdnpurge.New(cdnpurge.Options{
//			Cloudflare: &cdnpurge.Cloudflare{
//				ZoneID:      os.Getenv("CF_ZONE_ID"),
//				APITokenEnv: "CF_API_TOKEN",
//			},
//		})},
//	})
//
// When collage drops cached pages it names their paths; the plugin makes them
// absolute under the site's origin (Config.BaseURL, or each host's origin from an
// origin resolver) and asks the CDN to drop its copies too, so a reader
// behind the CDN sees the new page when a reader of the origin does. Cloudflare is
// purged by URL through its API, and anything else through a webhook that
// receives the URLs as JSON.
//
// Purging is never done by the goroutine that invalidated. What it names is
// collected for a short window — two seconds unless Window says otherwise — so a
// burst of invalidations is one request, and sent in the background, retried with
// backoff when the CDN answers 429 or 5xx. What is still pending when the
// application shuts down is sent before Shutdown returns. A development server
// purges nothing unless InDevelopment says so: its invalidations are an editor's, and the
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
	"os"
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
	// Cloudflare purges a Cloudflare zone. At least one of Cloudflare and
	// Webhook is required; with both, both are purged.
	Cloudflare *Cloudflare `json:"cloudflare"`
	// Webhook posts the URLs to an endpoint of your own, or of a CDN whose API
	// takes a list of URLs.
	Webhook *Webhook `json:"webhook"`
	// Window is how long invalidations are collected before they are sent, so a
	// burst of them is one request. Default "2s"; negative is an error.
	Window collage.Duration `json:"window"`
	// Retries is how many times a request the CDN answered 429 or 5xx, or that
	// did not reach it, is tried again. Default 4; negative is an error.
	Retries int `json:"retries"`
	// Backoff is the wait before the first retry, doubled before each one after
	// it. A 429's Retry-After, when longer, is honoured instead. Default "1s";
	// negative is an error.
	Backoff collage.Duration `json:"backoff"`
	// Timeout bounds one request to the CDN. Default "10s"; negative is an
	// error.
	Timeout collage.Duration `json:"timeout"`
	// InDevelopment purges from a development server too, which otherwise
	// purges nothing.
	InDevelopment bool `json:"inDevelopment"`
	// HTTPClient sends the requests. Default: an http.Client with no timeout of
	// its own, since each request is bounded by Timeout.
	HTTPClient *http.Client `json:"-"`
}

// Cloudflare purges files from one Cloudflare zone.
type Cloudflare struct {
	// ZoneID is the zone's identifier, on the zone's overview page. Required.
	ZoneID string `json:"zoneID"`
	// APITokenEnv names the environment variable holding an API token with the
	// Zone → Cache Purge permission for the zone. It or APIToken is required;
	// when set, it wins over APIToken.
	APITokenEnv string `json:"apiTokenEnv"`
	// APIToken is the token itself, set from Go.
	APIToken string `json:"-"`
	// BatchSize is how many URLs one purge request carries. Default 30, which is
	// what every Cloudflare plan accepts; an Enterprise zone takes more.
	// Negative is an error.
	BatchSize int `json:"batchSize"`
	// Endpoint is the API's base URL. Default "https://api.cloudflare.com".
	Endpoint string `json:"endpoint"`
}

// Webhook posts {"urls": [...]} to URL.
type Webhook struct {
	// URL receives the POST. Required.
	URL string `json:"url"`
	// TokenEnv names the environment variable holding a token sent as
	// "Authorization: Bearer <token>". Optional; when set, it wins over Token,
	// and an empty variable is an error.
	TokenEnv string `json:"tokenEnv"`
	// Token is the token itself, set from Go.
	Token string `json:"-"`
	// BatchSize is how many URLs one request carries. Default: all of them;
	// negative is an error.
	BatchSize int `json:"batchSize"`
}

// ErrNoBaseURL is returned by Init when the site has no origin to purge under:
// neither Config.BaseURL nor an origin resolver.
var ErrNoBaseURL = errors.New("cdnpurge: no origin to purge under: set Config.BaseURL (BASE_URL) or register an origin resolver")

// ErrNoProvider is returned by Init when neither Cloudflare nor Webhook is set.
var ErrNoProvider = errors.New("cdnpurge: nothing to purge: set Cloudflare or Webhook")

// Plugin purges the CDN.
type Plugin struct {
	opts    Options
	base    string
	origins collage.Host // names each entry's origin; nil when base is fixed
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
func (p *Plugin) Version() string { return "0.3.0" }

var _ collage.CacheInvalidateHook = (*Plugin)(nil)

// Init reads and checks the configuration. A plugin that cannot purge refuses to
// start rather than letting the CDN serve stale pages without a word.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	cfg, err := collage.PluginConfig(host, p.opts)
	if err != nil {
		return err
	}
	p.opts = cfg
	p.logger = host.Logger()
	o := &p.opts
	// Each entry is purged under the origin collage names for its host — a
	// resolver plugin's, else Config.BaseURL — so a tenant site that also sets
	// Config.BaseURL still purges each tenant's URLs under the tenant's origin.
	// With one origin for every host, it is named once, here.
	p.base, p.origins = "", nil
	switch {
	case host.MultiOrigin():
		p.origins = host
	case host.BaseURL() != "":
		p.base = strings.TrimSuffix(host.BaseURL(), "/")
	default:
		return ErrNoBaseURL
	}
	if o.Cloudflare == nil && o.Webhook == nil {
		return ErrNoProvider
	}
	if o.Retries < 0 {
		return fmt.Errorf("cdnpurge: retries is %d; write 0 for the default of 4, or a positive number", o.Retries)
	}
	for _, d := range []struct {
		key   string
		value collage.Duration
	}{{"window", o.Window}, {"backoff", o.Backoff}, {"timeout", o.Timeout}} {
		if d.value < 0 {
			return fmt.Errorf("cdnpurge: %s is %v; write 0 for the default, or a positive duration", d.key, time.Duration(d.value))
		}
	}
	if o.Window == 0 {
		o.Window = collage.Duration(2 * time.Second)
	}
	if o.Retries == 0 {
		o.Retries = 4
	}
	if o.Backoff == 0 {
		o.Backoff = collage.Duration(time.Second)
	}
	if o.Timeout == 0 {
		o.Timeout = collage.Duration(10 * time.Second)
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{}
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
	p.enabled = !host.DevMode() || o.InDevelopment
	p.pending = make(map[string]struct{})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return nil
}

// secret is the value of the environment variable env when it is named, else
// value. A named variable that is empty is an error, never a fallback.
func secret(env, value string) (string, error) {
	if env == "" {
		return value, nil
	}
	v := os.Getenv(env)
	if v == "" {
		return "", fmt.Errorf("cdnpurge: the environment variable %s is empty", env)
	}
	return v, nil
}

func cloudflare(cf Cloudflare) (target, error) {
	token, err := secret(cf.APITokenEnv, cf.APIToken)
	if err != nil {
		return target{}, err
	}
	cf.APIToken = token
	if cf.ZoneID == "" || cf.APIToken == "" {
		return target{}, errors.New("cdnpurge: Cloudflare needs both zoneID and apiTokenEnv (or APIToken from Go)")
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
	if cf.BatchSize < 0 {
		return target{}, fmt.Errorf("cdnpurge: cloudflare batchSize is %d; write 0 for the default of 30, or a positive number", cf.BatchSize)
	}
	batch := cf.BatchSize
	if batch == 0 {
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
	token, err := secret(wh.TokenEnv, wh.Token)
	if err != nil {
		return target{}, err
	}
	wh.Token = token
	if wh.BatchSize < 0 {
		return target{}, fmt.Errorf("cdnpurge: webhook batchSize is %d; write 0 to send every URL at once, or a positive number", wh.BatchSize)
	}
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
func (p *Plugin) OnCacheInvalidate(ctx context.Context, ev *collage.CacheInvalidateEvent) error {
	if !p.enabled || len(ev.Entries) == 0 {
		return nil
	}
	// Origins are named before the lock, once per host: OriginFor may be a
	// resolver's I/O, which must hold up neither a flush nor another
	// invalidation, and a resolver that invalidates in turn would deadlock.
	urls := make([]string, 0, len(ev.Entries))
	byHost := make(map[string]string)
	for _, entry := range ev.Entries {
		origin := p.base
		if origin == "" && p.origins != nil {
			var named bool
			if origin, named = byHost[entry.Host]; !named {
				origin = p.origins.OriginFor(ctx, entry.Host)
				byHost[entry.Host] = origin
			}
		}
		if origin == "" {
			continue
		}
		urls = append(urls, absolute(origin, entry.Path))
	}
	if len(urls) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	for _, u := range urls {
		p.pending[u] = struct{}{}
	}
	if p.timer == nil {
		p.timer = time.AfterFunc(time.Duration(p.opts.Window), p.flush)
	}
	return nil
}

// absolute is path under origin, escaped as a URL path: collage names paths as
// the router matched them, decoded.
func absolute(origin, path string) string {
	return origin + (&url.URL{Path: path}).EscapedPath()
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
	res, err := p.opts.HTTPClient.Do(req)
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
