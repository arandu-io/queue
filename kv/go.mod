module github.com/arandu-io/queue/kv

go 1.25

// Its own module, so a project that queues over its own database does not carry
// a Redis client in its go.sum, its build and its vulnerability surface.
require (
	github.com/arandu-io/framework v0.7.0
	github.com/redis/go-redis/v9 v9.7.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
)
