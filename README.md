# queue

Job queue for [Arandu](https://github.com/arandu-io/framework): work that
happens after the response, with retry, backoff and a dead letter queue.

The contract lives in the core, in `framework/jobs`. This repository is the two
default implementations of it.

```sh
go get github.com/arandu-io/queue        # over the application's database
go get github.com/arandu-io/queue/kv     # over RESP
```

## Which one

**The table**, unless you have a reason. A job pushed inside
`data.Transaction` is committed by the same transaction as the row it is about,
so it cannot refer to a write that rolled back — the outbox guarantee, applied
to work instead of to events. It also needs nothing installed.

**RESP** when the volume outgrows what a table handles comfortably. Same
`Worker`, same handlers, one line different in `main` — and no transactional
guarantee, which is the whole trade.

```go
q := queue.New(db)                          // or kvqueue.New(kvqueue.Options{…})

k := kernel.New(cfg).Register(queue.NewModule(q, "default", "mail"))

// pushing, from a service, inside the write:
j, _ := jobs.New(g, "mail", "invoice.send", invoice.ID)
_ = q.Push(ctx, g, j)

// draining, in `aru work`:
jobs.NewWorker(q, jobs.WorkerOptions{Queue: "mail"}).
    HandleFunc("invoice.send", sendInvoice)
```

Every job carries the `Grant` that pushed it — tenant, subject and action — and
the worker reissues the work under exactly that. There is no unauthorized path
into the database from a worker.

Delivery is at-least-once. A handler that cannot run twice safely is a handler
with a bug: the process can die between doing the work and acknowledging it,
and no queue anywhere solves that.

MIT. See [LICENSE.md](LICENSE.md).
