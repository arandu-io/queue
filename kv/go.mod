module github.com/arandu-io/queue/kv

go 1.25.0

// Its own module, so a project that queues over its own database does not carry
// a Redis client in its go.sum, its build and its vulnerability surface.
require (
	github.com/arandu-io/framework v0.13.2
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
)
