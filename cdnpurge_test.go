package cdnpurge_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	cdnpurge "github.com/Elagoht/collage-cdnpurge"
	"github.com/Elagoht/collage/pkg/collage"
)

// cdn is a fake purge API: it records every request and answers with the next
// status in statuses, then 200.
type cdn struct {
	*httptest.Server
	mu       sync.Mutex
	calls    []call
	statuses []int
	block    chan struct{}
	got      chan call
}

type call struct {
	path, auth string
	body       map[string][]string
}

func newCDN(t *testing.T, statuses ...int) *cdn {
	t.Helper()
	c := &cdn{statuses: statuses, got: make(chan call, 100)}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c.block != nil {
			<-c.block
		}
		raw, _ := io.ReadAll(r.Body)
		var body map[string][]string
		_ = json.Unmarshal(raw, &body)
		cl := call{path: r.URL.Path, auth: r.Header.Get("Authorization"), body: body}
		c.mu.Lock()
		c.calls = append(c.calls, cl)
		status := http.StatusOK
		if len(c.statuses) > 0 {
			status, c.statuses = c.statuses[0], c.statuses[1:]
		}
		c.mu.Unlock()
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "0")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"success": true, "errors": []}`)
		c.got <- cl
	}))
	t.Cleanup(c.Close)
	return c
}

func (c *cdn) wait(t *testing.T) call {
	t.Helper()
	select {
	case cl := <-c.got:
		return cl
	case <-time.After(5 * time.Second):
		t.Fatal("no purge request arrived")
		return call{}
	}
}

func (c *cdn) quiet(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case cl := <-c.got:
		t.Fatalf("unexpected purge request %+v", cl)
	case <-time.After(d):
	}
}

type site struct {
	app *collage.App
	h   http.Handler
}

// newSite builds an application with a cached page at /posts/{slug}, tagged
// "posts" and "post:<slug>".
func newSite(t *testing.T, dev bool, logs io.Writer, plugins ...collage.Plugin) *site {
	t.Helper()
	if logs == nil {
		logs = io.Discard
	}
	app, err := collage.New(&collage.Config{
		Server:   collage.ServerConfig{Host: "localhost", Port: 3000},
		Template: collage.TemplateConfig{FS: fstest.MapFS{"t/p.html": {Data: []byte(`<p>{{.}}</p>`)}}, Root: "t"},
		Cache:    collage.CacheConfig{Enabled: true, Type: "memory", DefaultTTL: time.Hour},
		DevMode:  dev,
		Logger:   slog.New(slog.NewTextHandler(logs, nil)),
		Plugins:  plugins,
	})
	if err != nil {
		t.Fatal(err)
	}
	post := collage.NewFragment("post", "p.html").WithDataHandler(func(_ context.Context, rc *collage.RenderContext) (any, []string, error) {
		return rc.Param("slug"), []string{"post:" + rc.Param("slug"), "posts"}, nil
	}).Static().Build()
	if err := app.RegisterPage(collage.NewPage("post").WithContent(post).WithPath("en", "/posts/{slug}").Build()); err != nil {
		t.Fatal(err)
	}
	s := &site{app: app, h: app.Handler()}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })
	return s
}

func (s *site) get(t *testing.T, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	s.h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func cloudflare(c *cdn) *cdnpurge.Cloudflare {
	return &cdnpurge.Cloudflare{ZoneID: "zone1", APIToken: "secret", Endpoint: c.URL}
}

func fast(o cdnpurge.Options) cdnpurge.Options {
	if o.Window == 0 {
		o.Window = cdnpurge.Duration(20 * time.Millisecond)
	}
	o.Backoff = cdnpurge.Duration(time.Millisecond)
	return o
}

func TestCloudflarePurgesTheDroppedURLs(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, nil, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com/", Cloudflare: cloudflare(c)})))
	s.get(t, "/posts/a")
	s.get(t, "/posts/b")
	s.get(t, "/posts/c%20d")
	if err := s.app.InvalidateTags(context.Background(), "posts"); err != nil {
		t.Fatal(err)
	}
	cl := c.wait(t)
	if cl.path != "/client/v4/zones/zone1/purge_cache" {
		t.Errorf("path = %q", cl.path)
	}
	if cl.auth != "Bearer secret" {
		t.Errorf("Authorization = %q", cl.auth)
	}
	want := []string{"https://example.com/posts/a", "https://example.com/posts/b", "https://example.com/posts/c%20d"}
	if !slices.Equal(cl.body["files"], want) {
		t.Errorf("files = %q, want %q", cl.body["files"], want)
	}
}

// A burst of invalidations inside the window is one request.
func TestInvalidationsInOneWindowAreOneRequest(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, nil, cdnpurge.New(fast(cdnpurge.Options{
		BaseURL: "https://example.com", Cloudflare: cloudflare(c), Window: cdnpurge.Duration(200 * time.Millisecond),
	})))
	s.get(t, "/posts/a")
	s.get(t, "/posts/b")
	ctx := context.Background()
	_ = s.app.InvalidateTags(ctx, "post:a")
	s.get(t, "/posts/a") // cached again, and dropped again below: named once
	_ = s.app.InvalidateTags(ctx, "post:b")
	_ = s.app.InvalidateTags(ctx, "post:a")
	cl := c.wait(t)
	if want := []string{"https://example.com/posts/a", "https://example.com/posts/b"}; !slices.Equal(cl.body["files"], want) {
		t.Errorf("files = %q, want %q", cl.body["files"], want)
	}
	c.quiet(t, 300*time.Millisecond)
}

// Cloudflare takes 30 URLs per request.
func TestCloudflareBatchesOf30(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, nil, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c)})))
	for i := range 65 {
		s.get(t, fmt.Sprintf("/posts/%02d", i))
	}
	_ = s.app.InvalidateTags(context.Background(), "posts")
	var sizes []int
	seen := map[string]bool{}
	for range 3 {
		cl := c.wait(t)
		sizes = append(sizes, len(cl.body["files"]))
		for _, u := range cl.body["files"] {
			seen[u] = true
		}
	}
	if !slices.Equal(sizes, []int{30, 30, 5}) || len(seen) != 65 {
		t.Errorf("batches = %v, distinct URLs = %d", sizes, len(seen))
	}
}

// 429 and 5xx are tried again; the batch arrives in the end.
func TestRetriesOn429And5xx(t *testing.T) {
	c := newCDN(t, http.StatusTooManyRequests, http.StatusBadGateway)
	s := newSite(t, false, nil, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c)})))
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	for range 3 {
		c.wait(t)
	}
	c.quiet(t, 100*time.Millisecond)
}

// A 400 will be refused however often it is sent: tried once, and logged.
func TestNoRetryOn4xx(t *testing.T) {
	c := newCDN(t, http.StatusBadRequest)
	var logs syncBuffer
	s := newSite(t, false, &logs, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c)})))
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	c.wait(t)
	c.quiet(t, 100*time.Millisecond)
	if !strings.Contains(logs.String(), "purge failed") {
		t.Errorf("the failure was not logged:\n%s", logs.String())
	}
}

// Retries stop at Retries, and the failure is logged.
func TestRetriesAreBounded(t *testing.T) {
	c := newCDN(t, 500, 500, 500, 500, 500, 500)
	var logs syncBuffer
	s := newSite(t, false, &logs, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c), Retries: 2})))
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	for range 3 {
		c.wait(t)
	}
	c.quiet(t, 100*time.Millisecond)
	if !strings.Contains(logs.String(), "purge failed") {
		t.Errorf("the failure was not logged:\n%s", logs.String())
	}
}

// Cloudflare refuses some requests with 200 and success: false.
func TestCloudflareSuccessFalseIsLogged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"success": false, "errors": [{"code": 1012, "message": "Request must contain one of \"purge_everything\" or \"files\""}]}`)
	}))
	defer srv.Close()
	var logs syncBuffer
	p := cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z", APIToken: "t", Endpoint: srv.URL}}))
	s := newSite(t, false, &logs, p)
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	_ = s.app.Shutdown(context.Background())
	if !strings.Contains(logs.String(), "1012") {
		t.Errorf("the refusal was not logged:\n%s", logs.String())
	}
}

// The invalidating goroutine never waits on the CDN.
func TestInvalidationDoesNotWaitForTheCDN(t *testing.T) {
	c := newCDN(t)
	c.block = make(chan struct{})
	s := newSite(t, false, nil, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c), Window: cdnpurge.Duration(time.Millisecond)})))
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	time.Sleep(50 * time.Millisecond) // the purge is now in flight, blocked
	s.get(t, "/posts/b")
	done := make(chan struct{})
	go func() {
		_ = s.app.InvalidateTags(context.Background(), "posts")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateTags waited for the CDN")
	}
	close(c.block)
	c.wait(t)
	c.wait(t)
}

// What is pending at shutdown is sent before Shutdown returns.
func TestShutdownFlushesPending(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, nil, cdnpurge.New(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c), Window: cdnpurge.Duration(time.Hour)}))
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	c.quiet(t, 50*time.Millisecond)
	if err := s.app.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case cl := <-c.got:
		if !slices.Equal(cl.body["files"], []string{"https://example.com/posts/a"}) {
			t.Errorf("files = %q", cl.body["files"])
		}
	default:
		t.Fatal("Shutdown returned before the pending purge was sent")
	}
}

// A retry still waiting when Shutdown's context ends gives up.
func TestShutdownGivesUpWhenItsContextEnds(t *testing.T) {
	c := newCDN(t, 500, 500, 500, 500, 500)
	o := cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c), Window: cdnpurge.Duration(time.Hour), Backoff: cdnpurge.Duration(time.Hour)}
	s := newSite(t, false, nil, cdnpurge.New(o))
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.app.Shutdown(ctx)
	if err == nil || !strings.Contains(err.Error(), "in flight") {
		t.Errorf("Shutdown = %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("Shutdown took %v", time.Since(start))
	}
}

func TestWebhook(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, nil, cdnpurge.New(fast(cdnpurge.Options{
		BaseURL: "https://example.com",
		Webhook: &cdnpurge.Webhook{URL: c.URL + "/purge", Token: "tok"},
	})))
	s.get(t, "/posts/a")
	s.get(t, "/posts/b")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	cl := c.wait(t)
	if cl.path != "/purge" || cl.auth != "Bearer tok" {
		t.Errorf("request = %q %q", cl.path, cl.auth)
	}
	if want := []string{"https://example.com/posts/a", "https://example.com/posts/b"}; !slices.Equal(cl.body["urls"], want) {
		t.Errorf("urls = %q", cl.body["urls"])
	}
}

// Configured with both, both are purged.
func TestBothProviders(t *testing.T) {
	cf, wh := newCDN(t), newCDN(t)
	s := newSite(t, false, nil, cdnpurge.New(fast(cdnpurge.Options{
		BaseURL: "https://example.com", Cloudflare: cloudflare(cf), Webhook: &cdnpurge.Webhook{URL: wh.URL},
	})))
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	cf.wait(t)
	if cl := wh.wait(t); cl.auth != "" {
		t.Errorf("a webhook without a token sent Authorization %q", cl.auth)
	}
}

// An invalidation that dropped nothing purges nothing.
func TestNothingDroppedNothingPurged(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, nil, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c)})))
	_ = s.app.InvalidateTags(context.Background(), "posts")
	c.quiet(t, 100*time.Millisecond)
}

// A development server purges nothing, unless forced.
func TestDevelopment(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, true, nil, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c)})))
	s.get(t, "/posts/a")
	_ = s.app.InvalidateTags(context.Background(), "posts")
	_ = s.app.Shutdown(context.Background())
	c.quiet(t, 100*time.Millisecond)

	forced := newSite(t, true, nil, cdnpurge.New(fast(cdnpurge.Options{BaseURL: "https://example.com", Cloudflare: cloudflare(c), Force: true})))
	forced.get(t, "/posts/a")
	_ = forced.app.InvalidateTags(context.Background(), "posts")
	c.wait(t)
}

// Configuration read from JSON: durations as Go writes them.
func TestJSONConfiguration(t *testing.T) {
	c := newCDN(t)
	raw := fmt.Sprintf(`{"baseURL": "https://example.com", "window": "10ms", "backoff": "1ms",
		"cloudflare": {"zoneID": "z9", "apiToken": "tk", "endpoint": %q}}`, c.URL)
	app, err := collage.New(&collage.Config{
		Server:       collage.ServerConfig{Host: "localhost", Port: 3000},
		Template:     collage.TemplateConfig{FS: fstest.MapFS{"t/p.html": {Data: []byte(`<p>x</p>`)}}, Root: "t"},
		Cache:        collage.CacheConfig{Enabled: true, Type: "memory", DefaultTTL: time.Hour},
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Plugins:      []collage.Plugin{cdnpurge.New(cdnpurge.Options{})},
		PluginConfig: map[string]json.RawMessage{cdnpurge.Name: json.RawMessage(raw)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.RegisterPage(collage.NewPage("home").WithContent(collage.NewFragment("home", "p.html").Static().Build()).WithPath("en", "/").Build()); err != nil {
		t.Fatal(err)
	}
	defer app.Shutdown(context.Background())
	h := app.Handler()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	_ = app.InvalidateTags(context.Background(), collage.PathTag("/"))
	if cl := c.wait(t); cl.path != "/client/v4/zones/z9/purge_cache" || !slices.Equal(cl.body["files"], []string{"https://example.com/"}) {
		t.Errorf("request = %+v", cl)
	}
}

// A plugin that cannot purge stops the application from starting.
func TestMisconfigurationStopsStartup(t *testing.T) {
	cases := map[string]cdnpurge.Options{
		"no base URL":         {Webhook: &cdnpurge.Webhook{URL: "https://hooks.example.com"}},
		"relative base URL":   {BaseURL: "example.com", Webhook: &cdnpurge.Webhook{URL: "https://hooks.example.com"}},
		"no provider":         {BaseURL: "https://example.com"},
		"no token":            {BaseURL: "https://example.com", Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z"}},
		"no zone":             {BaseURL: "https://example.com", Cloudflare: &cdnpurge.Cloudflare{APIToken: "t"}},
		"webhook without URL": {BaseURL: "https://example.com", Webhook: &cdnpurge.Webhook{}},
		"bad endpoint":        {BaseURL: "https://example.com", Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z", APIToken: "t", Endpoint: "api.cloudflare.com"}},
	}
	for name, o := range cases {
		t.Run(name, func(t *testing.T) {
			s := newSite(t, false, nil, cdnpurge.New(o))
			if code := s.get(t, "/posts/a"); code != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503", code)
			}
		})
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
