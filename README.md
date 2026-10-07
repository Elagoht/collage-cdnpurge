# elagoht/cdnpurge

A collage plugin that purges a CDN's copies of the pages collage invalidates:
Cloudflare by zone, or any service that takes a list of URLs through a webhook.
When a post is saved and its tag invalidated, collage drops its cached pages and
names their paths; this plugin makes them absolute and tells the CDN to drop them
too, so a reader behind the CDN is not served yesterday's page for the rest of the
day.

```go
app, err := collage.New(&collage.Config{
	Plugins: []collage.Plugin{cdnpurge.New(cdnpurge.Options{
		BaseURL: "https://example.com",
		Cloudflare: &cdnpurge.Cloudflare{
			ZoneID:   os.Getenv("CF_ZONE_ID"),
			APIToken: os.Getenv("CF_API_TOKEN"),
		},
	})},
})
```

Requires collage v0.50.0 or later: it reads `CacheInvalidateEvent.Paths`, which
v0.23.0 added.

Registering it is the whole of it. Nothing changes in a page or a template.

## What is purged

Exactly the paths collage says it dropped, under `BaseURL`: an invalidation of
`posts` that dropped `/posts/a` and `/tr/yazilar/a` purges
`https://example.com/posts/a` and `https://example.com/tr/yazilar/a`. A page that
was not cached is not named, because nothing of it was dropped — which also means
a page the CDN holds but the origin's cache did not is not purged. Make sure
what the CDN caches, collage caches too.

`collage.PathTag` invalidates one path by itself, and is purged the same way:

```go
app.InvalidateTags(ctx, collage.PathTag("/about"))
```

## Batching, retries, shutdown

Purging never happens on the goroutine that invalidated. That goroutine is often a
request's — the action that saved the post — and a CDN's API is not something it
should wait on.

- **Batched.** What is invalidated is collected for `Window` (two seconds by
  default), de-duplicated, and sent as one request. A save that invalidates five
  tags is one purge, not five.
- **Split.** Cloudflare accepts 30 URLs per request on every plan, so larger sets
  are sent 30 at a time; an Enterprise zone can raise `batchSize`. A webhook
  receives everything in one request unless its own `batchSize` says otherwise.
- **Retried.** A `429`, a `5xx`, or a request that never got an answer is tried
  again after `Backoff`, doubled each time, up to `Retries` times. A `429`'s
  `Retry-After`, when longer, is honoured instead. Any other `4xx` is a request
  the CDN will refuse however often it is made: it is tried once.
- **Logged.** A purge that finally fails is logged through the application's
  logger, with the provider and the number of URLs, and nothing else happens —
  an invalidation has already succeeded at the origin and is not undone.
- **Flushed.** `Shutdown` sends whatever is still waiting for its window and waits
  for purges in flight, until its context ends. A retry still sleeping then gives
  up, and `Shutdown` says so in its error.

## Providers

### Cloudflare

`POST /client/v4/zones/{zoneID}/purge_cache` with `{"files": [...]}` and the token
as a bearer. The token needs the **Zone → Cache Purge** permission for the zone,
and nothing more. Cloudflare answers some refusals with `200` and
`"success": false`; those are logged like any other failure.

### Webhook

`POST` to `url` with

```json
{ "urls": ["https://example.com/posts/a", "https://example.com/posts/b"] }
```

and `Authorization: Bearer <token>` when a token is set. Any `2xx` is success. It
is the adapter for everything else: a small function in front of Fastly's or
Bunny's API, a queue, a log.

With both configured, both are purged.

## Development

A development server purges nothing: its invalidations are an editor's, and the
CDN belongs to production. `force` purges anyway — for trying the plugin out
against a staging zone.

## Options

| Option | JSON | Default | |
| --- | --- | --- | --- |
| `BaseURL` | `baseURL` | `Config.BaseURL` | The public origin the CDN serves the site at; without it, `Config.BaseURL`, or each host's origin from an origin resolver |
| `Cloudflare` | `cloudflare` | | `zoneID`, `apiToken` (both required), `batchSize` (30), `endpoint` (`https://api.cloudflare.com`) |
| `Webhook` | `webhook` | | `url` (required), `token`, `batchSize` (all) |
| `Window` | `window` | `"2s"` | How long invalidations are collected before a purge |
| `Retries` | `retries` | `4` | Retries after a `429`, a `5xx` or no answer; negative tries once |
| `Backoff` | `backoff` | `"1s"` | The first retry's wait, doubled after each |
| `Timeout` | `timeout` | `"10s"` | One request's limit |
| `Force` | `force` | `false` | Purge from a development server too |
| `Client` | — | | The `*http.Client` requests are sent with |

At least one of `Cloudflare` and `Webhook` is required. No origin at all (no `BaseURL`, no `Config.BaseURL`, no origin resolver), a
provider without what it needs, or an endpoint that is not an absolute URL stops
the application from starting. Durations are written as Go writes them, `"2s"` or
`"500ms"`. `endpoint` exists so a test can point the plugin at a server of its own.

## Configuration

```json
{
  "elagoht/cdnpurge": {
    "baseURL": "https://example.com",
    "cloudflare": { "zoneID": "023e105f4ecef8ad9ca31a8372d0c353", "apiToken": "..." },
    "window": "2s",
    "retries": 4
  }
}
```

Keep the token out of a file under version control: set it in Go from the
environment, or fill `PluginConfig` from wherever your secrets live.

## Limitations

- **Paths, not query strings.** collage names the paths it dropped. A CDN that
  caches `/search?q=a` and `/search?q=b` as separate objects is asked to purge
  `/search` only. Cloudflare's purge by URL matches the exact URL, query included,
  so those variants stay until they expire.
- **Several hosts.** Without a `BaseURL` of its own, the plugin purges each
  invalidated entry under its host's origin, as a `collage.OriginResolver` plugin
  (such as `elagoht/tenant`) names it, falling back to `Config.BaseURL`. Two hosts
  of one origin purge the URL once. With `BaseURL` set, every path is purged under
  that one origin.
- **One zone.** A Cloudflare zone purges only its own hostnames, so tenants on
  custom domains in other zones need a webhook.
- **Best effort.** Pending purges live in memory. A process killed without a
  graceful shutdown loses what was waiting for its window, and a purge that fails
  every retry is logged, not queued for later.
- **No purge-everything, no tags.** Cloudflare's cache-tag and prefix purges exist
  on some plans; collage names URLs, and URLs are what is purged.
- **Invalidations only.** A page whose cache entry simply expires is not purged:
  nothing named it, and the CDN's own TTL is what governs it then.
