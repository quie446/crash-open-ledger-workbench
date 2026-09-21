package crashreplay

import pebble "github.com/cockroachdb/pebble"

// The crash-replay facility drives pebble's own write and restart paths; these
// aliases keep the rest of the package free of stuttering imports.
type (
	Options     = pebble.Options
	IterOptions = pebble.IterOptions
)

var (
	Sync   = pebble.Sync
	NoSync = pebble.NoSync
)

var ErrNotFound = pebble.ErrNotFound

// Open is pebble.Open.
var Open = pebble.Open
