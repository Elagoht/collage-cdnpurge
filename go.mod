// A collage plugin that purges a CDN's copies of the pages collage invalidates:
// Cloudflare by zone, or any service behind a webhook — batched, retried, and
// never in the way of the request that invalidated.
//
// It requires collage the way any consumer does, and reaches nothing the framework
// does not offer every plugin.
module github.com/Elagoht/collage-cdnpurge

go 1.26

require github.com/Elagoht/collage v0.50.0

retract v0.2.2 // tagged by mistake on the previous release's code; use v0.2.3 or later
