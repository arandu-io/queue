<h1 align="center">arandu-io/queue</h1>

<p align="center">The job queue for Arandu.</p>

<p align="center">
<a href="https://github.com/arandu-io/queue/actions/workflows/ci.yml"><img src="https://github.com/arandu-io/queue/actions/workflows/ci.yml/badge.svg" alt="Build Status"></a>
<a href="https://pkg.go.dev/github.com/arandu-io/queue"><img src="https://pkg.go.dev/badge/github.com/arandu-io/queue.svg" alt="Go Reference"></a>
<a href="https://github.com/arandu-io/queue/tags"><img src="https://img.shields.io/github/v/tag/arandu-io/queue?label=version" alt="Latest Version"></a>
<a href="LICENSE.md"><img src="https://img.shields.io/github/license/arandu-io/queue" alt="License"></a>
</p>

## About the queue

The default driver is a table in the application's own database, and that is the
point: a job pushed inside a transaction is committed by the same transaction as
the row that produced it. No job for a row that was rolled back, and no row
without its job.

```go
import _ "github.com/arandu-io/queue/kv"   // the RESP driver, in its own module
```

Exponential backoff and a dead-letter queue. The worker is the same binary with
another argument — `aru queue:work` — which keeps a deployment to one artifact
and stops the worker running a different build from the server.

## Learning Arandu

The API reference is generated from the doc comments and lives on
[pkg.go.dev](https://pkg.go.dev/github.com/arandu-io/framework). Every exported
symbol carries one, and that is deliberate: it is the documentation that cannot
drift from the code, because it sits in the same file.

The CLI documents itself — `aru help` lists every command, and each one explains
what it writes and what to do with it. `aru doctor` explains what it found and
what breaks, not which rule was violated.

A guide and a website do not exist yet, and that is a decision rather than a
gap: a guide written against an API that still moves is work done twice, and the
second time is worse — there is wrong documentation published. The site is the
next phase, and it will be an Arandu application.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Before opening a pull request, the three
commands at the top of that file have to pass, and CI runs exactly them.

## Security Vulnerabilities

Please review [our security policy](SECURITY.md) on how to report a
vulnerability. Never open a public issue for one.

## License

Open-sourced software licensed under the [MIT license](LICENSE.md).
