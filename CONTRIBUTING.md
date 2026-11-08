# Contributing

Use Go 1.26 or later on macOS or Linux. Run these commands from the repository root before opening a pull request.

```sh
go test ./...
go vet ./...
python3 .github/smoke.py
```

The smoke test builds the example and runner, interrupts the process after a checkpoint, then checks the recovered report. It uses a temporary directory.

For a bug, include the Go version, OS and architecture, DuraRun version, and a short set of steps that reproduces it. Share only logs you can publish. Use the [security policy](SECURITY.md) for vulnerabilities.

Keep each pull request focused and describe how you checked it. If your change affects checkpoint or resume behavior, cover the relevant failure or recovery case. Update both READMEs if an example changes.
