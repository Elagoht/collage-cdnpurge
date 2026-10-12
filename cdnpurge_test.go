package cdnpurge_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdnpurge "github.com/Elagoht/collage-cdnpurge"
	"github.com/Elagoht/collage/pkg/collage"
	"github.com/Elagoht/collage/pkg/collagetest"
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

// postPage is a cached page at /posts/{slug}, tagged "posts" and "post:<slug>".
func postPage() collage.Registrable {
	post := collage.NewFragment("post", "p.html").WithData(collage.LoadTagged(func(_ context.Context, rc *collage.RenderContext) (string, []string, error) {
		return rc.Param("slug"), []string{"post:" + rc.Param("slug"), "posts"}, nil
	})).Static().Build()
	return collage.NewPage("post").WithContent(post).WithPath("en", "/posts/{slug}").Build()
}

// siteOptions is what every site here has: the post page, a memory cache,
// baseURL as Config.BaseURL and dev as development mode.
func siteOptions(baseURL string, dev bool, plugins []collage.Plugin) []collagetest.SiteOption {
	return []collagetest.SiteOption{
		collagetest.WithPlugins(plugins...),
		collagetest.WithTemplates(map[string]string{"p.html": `<p>{{.}}</p>`}),
		collagetest.WithRegister(postPage()),
		collagetest.WithConfig(func(c *collage.Config) {
			c.BaseURL = baseURL
			c.DevMode = dev
			c.Cache = collage.CacheConfig{Enabled: true, Type: "memory", DefaultTTL: time.Hour}
		}),
	}
}

// newSite starts an application at https://example.com with the post page.
func newSite(t *testing.T, dev bool, plugins ...collage.Plugin) *collagetest.Site {
	t.Helper()
	return collagetest.NewSite(t, siteOptions("https://example.com", dev, plugins)...)
}

// siteAt is newSite with Config.BaseURL set to baseURL.
func siteAt(t *testing.T, baseURL string, plugins ...collage.Plugin) *collagetest.Site {
	t.Helper()
	return collagetest.NewSite(t, siteOptions(baseURL, false, plugins)...)
}

func cloudflare(c *cdn) *cdnpurge.Cloudflare {
	return &cdnpurge.Cloudflare{ZoneID: "zone1", APIToken: "secret", Endpoint: c.URL}
}

func fast(o cdnpurge.Options) cdnpurge.Options {
	if o.Window == 0 {
		o.Window = collage.Duration(20 * time.Millisecond)
	}
	o.Backoff = collage.Duration(time.Millisecond)
	return o
}

func TestCloudflarePurgesTheDroppedURLs(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	s.Get("/posts/a")
	s.Get("/posts/b")
	s.Get("/posts/c%20d")
	if err := s.App.InvalidateTags(context.Background(), "posts"); err != nil {
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
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{
		Cloudflare: cloudflare(c), Window: collage.Duration(200 * time.Millisecond),
	})))
	s.Get("/posts/a")
	s.Get("/posts/b")
	ctx := context.Background()
	_ = s.App.InvalidateTags(ctx, "post:a")
	s.Get("/posts/a") // cached again, and dropped again below: named once
	_ = s.App.InvalidateTags(ctx, "post:b")
	_ = s.App.InvalidateTags(ctx, "post:a")
	cl := c.wait(t)
	if want := []string{"https://example.com/posts/a", "https://example.com/posts/b"}; !slices.Equal(cl.body["files"], want) {
		t.Errorf("files = %q, want %q", cl.body["files"], want)
	}
	c.quiet(t, 300*time.Millisecond)
}

// Cloudflare takes 30 URLs per request.
func TestCloudflareBatchesOf30(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	for i := range 65 {
		s.Get(fmt.Sprintf("/posts/%02d", i))
	}
	_ = s.App.InvalidateTags(context.Background(), "posts")
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
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	for range 3 {
		c.wait(t)
	}
	c.quiet(t, 100*time.Millisecond)
}

// A 400 will be refused however often it is sent: tried once, and logged.
func TestNoRetryOn4xx(t *testing.T) {
	c := newCDN(t, http.StatusBadRequest)
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	c.wait(t)
	c.quiet(t, 100*time.Millisecond)
	if !s.Logs.Has(slog.LevelError, "purge failed") {
		t.Errorf("the failure was not logged:\n%s", s.Logs.String())
	}
}

// Retries stop at Retries, and the failure is logged.
func TestRetriesAreBounded(t *testing.T) {
	c := newCDN(t, 500, 500, 500, 500, 500, 500)
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c), Retries: 2})))
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	for range 3 {
		c.wait(t)
	}
	c.quiet(t, 100*time.Millisecond)
	if !s.Logs.Has(slog.LevelError, "purge failed") {
		t.Errorf("the failure was not logged:\n%s", s.Logs.String())
	}
}

// Cloudflare refuses some requests with 200 and success: false.
func TestCloudflareSuccessFalseIsLogged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"success": false, "errors": [{"code": 1012, "message": "Request must contain one of \"purge_everything\" or \"files\""}]}`)
	}))
	defer srv.Close()
	p := cdnpurge.New(fast(cdnpurge.Options{Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z", APIToken: "t", Endpoint: srv.URL}}))
	s := newSite(t, false, p)
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	_ = s.App.Shutdown(context.Background())
	if !s.Logs.Has(slog.LevelError, "1012") {
		t.Errorf("the refusal was not logged:\n%s", s.Logs.String())
	}
}

// The invalidating goroutine never waits on the CDN.
func TestInvalidationDoesNotWaitForTheCDN(t *testing.T) {
	c := newCDN(t)
	c.block = make(chan struct{})
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c), Window: collage.Duration(time.Millisecond)})))
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	time.Sleep(50 * time.Millisecond) // the purge is now in flight, blocked
	s.Get("/posts/b")
	done := make(chan struct{})
	go func() {
		_ = s.App.InvalidateTags(context.Background(), "posts")
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
	s := newSite(t, false, cdnpurge.New(cdnpurge.Options{Cloudflare: cloudflare(c), Window: collage.Duration(time.Hour)}))
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	c.quiet(t, 50*time.Millisecond)
	if err := s.App.Shutdown(context.Background()); err != nil {
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
	o := cdnpurge.Options{Cloudflare: cloudflare(c), Window: collage.Duration(time.Hour), Backoff: collage.Duration(time.Hour)}
	p := cdnpurge.New(o)
	s := newSite(t, false, p)
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	// The plugin's own Shutdown, not the application's: an application keeps
	// its first Shutdown's error, which the site's cleanup would then report.
	err := p.Shutdown(ctx)
	if err == nil || !strings.Contains(err.Error(), "in flight") {
		t.Errorf("Shutdown = %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("Shutdown took %v", time.Since(start))
	}
}

func TestWebhook(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{
		Webhook: &cdnpurge.Webhook{URL: c.URL + "/purge", Token: "tok"},
	})))
	s.Get("/posts/a")
	s.Get("/posts/b")
	_ = s.App.InvalidateTags(context.Background(), "posts")
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
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{
		Cloudflare: cloudflare(cf), Webhook: &cdnpurge.Webhook{URL: wh.URL},
	})))
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	cf.wait(t)
	if cl := wh.wait(t); cl.auth != "" {
		t.Errorf("a webhook without a token sent Authorization %q", cl.auth)
	}
}

// An invalidation that dropped nothing purges nothing.
func TestNothingDroppedNothingPurged(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	_ = s.App.InvalidateTags(context.Background(), "posts")
	c.quiet(t, 100*time.Millisecond)
}

// A development server purges nothing, unless forced.
func TestDevelopment(t *testing.T) {
	c := newCDN(t)
	s := newSite(t, true, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	_ = s.App.Shutdown(context.Background())
	c.quiet(t, 100*time.Millisecond)

	forced := newSite(t, true, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c), InDevelopment: true})))
	forced.Get("/posts/a")
	_ = forced.App.InvalidateTags(context.Background(), "posts")
	c.wait(t)
}

// Configuration read from JSON: durations as Go writes them, the token from the
// environment variable apiTokenEnv names.
func TestJSONConfiguration(t *testing.T) {
	c := newCDN(t)
	t.Setenv("TEST_CF_TOKEN", "tk")
	raw := fmt.Sprintf(`{"window": "10ms", "backoff": "1ms",
		"cloudflare": {"zoneID": "z9", "apiTokenEnv": "TEST_CF_TOKEN", "endpoint": %q}}`, c.URL)
	s := collagetest.NewSite(t, append(siteOptions("https://example.com", false, []collage.Plugin{cdnpurge.New(cdnpurge.Options{})}),
		collagetest.WithPluginConfig(cdnpurge.Name, raw))...)
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	cl := c.wait(t)
	if cl.path != "/client/v4/zones/z9/purge_cache" || !slices.Equal(cl.body["files"], []string{"https://example.com/posts/a"}) {
		t.Errorf("request = %+v", cl)
	}
	if cl.auth != "Bearer tk" {
		t.Errorf("Authorization = %q, want the token from TEST_CF_TOKEN", cl.auth)
	}
}

// The webhook's token comes from the environment variable tokenEnv names, and
// wins over a token set from Go.
func TestWebhookTokenEnv(t *testing.T) {
	c := newCDN(t)
	t.Setenv("TEST_HOOK_TOKEN", "from-env")
	s := newSite(t, false, cdnpurge.New(fast(cdnpurge.Options{
		Webhook: &cdnpurge.Webhook{URL: c.URL, TokenEnv: "TEST_HOOK_TOKEN", Token: "from-go"},
	})))
	s.Get("/posts/a")
	_ = s.App.InvalidateTags(context.Background(), "posts")
	if cl := c.wait(t); cl.auth != "Bearer from-env" {
		t.Errorf("Authorization = %q, want the token from TEST_HOOK_TOKEN", cl.auth)
	}
}

// A plugin that cannot purge stops the application from starting.
func TestMisconfigurationStopsStartup(t *testing.T) {
	t.Setenv("TEST_EMPTY_TOKEN", "")
	cases := map[string]cdnpurge.Options{
		"no provider":         {},
		"no token":            {Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z"}},
		"empty token env":     {Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z", APITokenEnv: "TEST_EMPTY_TOKEN", APIToken: "t"}},
		"empty webhook env":   {Webhook: &cdnpurge.Webhook{URL: "https://hooks.example.com", TokenEnv: "TEST_EMPTY_TOKEN"}},
		"no zone":             {Cloudflare: &cdnpurge.Cloudflare{APIToken: "t"}},
		"webhook without URL": {Webhook: &cdnpurge.Webhook{}},
		"bad endpoint":        {Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z", APIToken: "t", Endpoint: "api.cloudflare.com"}},
		"negative retries":    {Webhook: &cdnpurge.Webhook{URL: "https://hooks.example.com"}, Retries: -1},
		"negative window":     {Webhook: &cdnpurge.Webhook{URL: "https://hooks.example.com"}, Window: collage.Duration(-time.Second)},
		"negative backoff":    {Webhook: &cdnpurge.Webhook{URL: "https://hooks.example.com"}, Backoff: collage.Duration(-time.Second)},
		"negative timeout":    {Webhook: &cdnpurge.Webhook{URL: "https://hooks.example.com"}, Timeout: collage.Duration(-time.Second)},
		"negative cf batch":   {Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z", APIToken: "t", BatchSize: -1}},
		"negative hook batch": {Webhook: &cdnpurge.Webhook{URL: "https://hooks.example.com", BatchSize: -1}},
	}
	for name, o := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := collagetest.TrySite(t, siteOptions("https://example.com", false, []collage.Plugin{cdnpurge.New(o)})...)
			if err == nil || !strings.Contains(err.Error(), "cdnpurge:") {
				t.Errorf("start = %v, want a cdnpurge error", err)
			}
		})
	}
}

// The keys v0.3.0 renamed or removed stop the application, by name.
func TestRemovedKeysFail(t *testing.T) {
	cases := map[string]string{
		"baseURL":  `{"baseURL": "https://example.com", "webhook": {"url": "https://hooks.example.com"}}`,
		"force":    `{"force": true, "webhook": {"url": "https://hooks.example.com"}}`,
		"apiToken": `{"cloudflare": {"zoneID": "z", "apiToken": "t"}}`,
		"token":    `{"webhook": {"url": "https://hooks.example.com", "token": "t"}}`,
	}
	for key, raw := range cases {
		t.Run(key, func(t *testing.T) {
			_, err := collagetest.TrySite(t, append(siteOptions("https://example.com", false, []collage.Plugin{cdnpurge.New(cdnpurge.Options{})}),
				collagetest.WithPluginConfig(cdnpurge.Name, raw))...)
			if err == nil || !strings.Contains(err.Error(), `"`+key+`"`) {
				t.Errorf("start = %v, want an error naming %q", err, key)
			}
		})
	}
}

// A duration written as a number is refused, not read as nanoseconds.
func TestDurationAsNumberFails(t *testing.T) {
	_, err := collagetest.TrySite(t, append(siteOptions("https://example.com", false, []collage.Plugin{cdnpurge.New(cdnpurge.Options{})}),
		collagetest.WithPluginConfig(cdnpurge.Name, `{"window": 2, "webhook": {"url": "https://hooks.example.com"}}`))...)
	if err == nil || !strings.Contains(err.Error(), `"window"`) {
		t.Errorf("start = %v, want an error naming window", err)
	}
}

// origins resolves two hosts, as elagoht/tenant would.
type origins struct{}

func (origins) Name() string                             { return "test/origins" }
func (origins) Version() string                          { return "0" }
func (origins) Init(context.Context, collage.Host) error { return nil }
func (origins) Shutdown(context.Context) error           { return nil }
func (origins) Origin(_ context.Context, host string) (string, bool) {
	switch host {
	case "a.test":
		return "https://a.example", true
	case "b.test":
		return "https://b.example", true
	}
	return "", false
}

// cdnpurge purges under Config.BaseURL.
func TestFallsBackToConfigBaseURL(t *testing.T) {
	c := newCDN(t)
	s := siteAt(t, "https://example.com", cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	s.Get("/posts/a")
	if err := s.App.InvalidateTags(context.Background(), "posts"); err != nil {
		t.Fatal(err)
	}
	if got := c.wait(t).body["files"]; !slices.Equal(got, []string{"https://example.com/posts/a"}) {
		t.Errorf("files = %q, want Config.BaseURL's", got)
	}
}

// With a resolver and no Config.BaseURL, each entry is purged under its host's origin,
// and two hosts of one origin purge the URL once.
func TestPurge_PerOrigin(t *testing.T) {
	c := newCDN(t)
	s := siteAt(t, "", origins{}, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	for _, url := range []string{"http://a.test/posts/a", "http://b.test/posts/a", "http://a.test:80/posts/a"} {
		s.Get(url)
	}
	if err := s.App.InvalidateTags(context.Background(), "posts"); err != nil {
		t.Fatal(err)
	}
	got := slices.Clone(c.wait(t).body["files"])
	slices.Sort(got)
	if want := []string{"https://a.example/posts/a", "https://b.example/posts/a"}; !slices.Equal(got, want) {
		t.Errorf("files = %q, want %q", got, want)
	}
	c.quiet(t, 100*time.Millisecond)
}

// With no Config.BaseURL and no resolver, collage cannot
// name an origin: starting the application fails with ErrNoBaseURL.
func TestNoOriginAnywhereFailsStart(t *testing.T) {
	_, err := collagetest.TrySite(t, siteOptions("", false, []collage.Plugin{cdnpurge.New(fast(cdnpurge.Options{Cloudflare: &cdnpurge.Cloudflare{ZoneID: "z", APIToken: "t"}}))})...)
	if !errors.Is(err, cdnpurge.ErrNoBaseURL) {
		t.Errorf("start = %v, want ErrNoBaseURL", err)
	}
}

// A site with Config.BaseURL and a resolver purges each entry under its host's
// origin; Config.BaseURL is only the fallback for a host no resolver knows.
func TestPurge_PerOriginWithConfigBaseURL(t *testing.T) {
	c := newCDN(t)
	s := siteAt(t, "https://main.example", origins{}, cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)})))
	for _, url := range []string{"http://a.test/posts/a", "http://b.test/posts/a", "http://other.test/posts/a"} {
		s.Get(url)
	}
	if err := s.App.InvalidateTags(context.Background(), "posts"); err != nil {
		t.Fatal(err)
	}
	got := slices.Clone(c.wait(t).body["files"])
	slices.Sort(got)
	want := []string{"https://a.example/posts/a", "https://b.example/posts/a", "https://main.example/posts/a"}
	if !slices.Equal(got, want) {
		t.Errorf("files = %q, want %q", got, want)
	}
}

// countingOrigins resolves every host to https://<host>, counting the calls; for
// "reenter.test" it invalidates again from inside the resolver, as a resolver
// that refreshes its own records might.
type countingOrigins struct {
	calls  atomic.Int32
	target collage.CacheInvalidateHook
}

func (*countingOrigins) Name() string                             { return "test/counting" }
func (*countingOrigins) Version() string                          { return "0" }
func (*countingOrigins) Init(context.Context, collage.Host) error { return nil }
func (*countingOrigins) Shutdown(context.Context) error           { return nil }
func (o *countingOrigins) Origin(ctx context.Context, host string) (string, bool) {
	o.calls.Add(1)
	if host == "reenter.test" {
		_ = o.target.OnCacheInvalidate(ctx, &collage.CacheInvalidateEvent{
			Entries: []collage.InvalidatedEntry{{Host: "a.test", Path: "/inner"}}})
	}
	return "https://" + host, true
}

// Each distinct host is resolved once per invalidation, however many of its
// entries were dropped.
func TestPurge_ResolvesEachHostOnce(t *testing.T) {
	c := newCDN(t)
	o := &countingOrigins{}
	p := cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)}))
	siteAt(t, "", o, p)
	o.calls.Store(0)
	err := p.OnCacheInvalidate(context.Background(), &collage.CacheInvalidateEvent{Entries: []collage.InvalidatedEntry{
		{Host: "a.test", Path: "/1"}, {Host: "a.test", Path: "/2"}, {Host: "a.test", Path: "/3"}, {Host: "b.test", Path: "/1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if n := o.calls.Load(); n != 2 {
		t.Errorf("resolver called %d times, want 2 (once per host)", n)
	}
	c.wait(t)
}

// The resolver runs outside the plugin's lock: one that invalidates again from
// inside does not deadlock.
func TestPurge_ResolverRunsOutsideTheLock(t *testing.T) {
	c := newCDN(t)
	o := &countingOrigins{}
	p := cdnpurge.New(fast(cdnpurge.Options{Cloudflare: cloudflare(c)}))
	o.target = p
	siteAt(t, "", o, p)
	done := make(chan error, 1)
	go func() {
		done <- p.OnCacheInvalidate(context.Background(), &collage.CacheInvalidateEvent{
			Entries: []collage.InvalidatedEntry{{Host: "reenter.test", Path: "/outer"}}})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnCacheInvalidate deadlocked: the resolver ran under the plugin's lock")
	}
	got := slices.Clone(c.wait(t).body["files"])
	slices.Sort(got)
	if want := []string{"https://a.test/inner", "https://reenter.test/outer"}; !slices.Equal(got, want) {
		t.Errorf("files = %q, want %q", got, want)
	}
}
