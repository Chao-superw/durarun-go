# DuraRun Go SDK Preview

DuraRun lets Go Agent developers save progress at application-defined steps and resume a run after the Agent process stops. The Agent keeps its own model, tools and workflow. The SDK records step completion and requests checkpoints. The local runner starts the process, supplies its workspace and checkpoint connection, and restores the saved workspace when asked to resume.

## Integrate an existing Agent

Use Go 1.26 or later. In a separate Go module, point to a local checkout:

```go
require github.com/Chao-superw/durarun-go v0.0.0
replace github.com/Chao-superw/durarun-go => /absolute/path/to/durarun-go
```

Import `github.com/Chao-superw/durarun-go/runtime` and adapt the existing entry point. The following sketch assumes the Agent already has an `inspect` function that writes notes to the current workspace and a `writeReport` function that reads those notes.

```go
rt, err := sdk.New(sdk.Config{Durable: true})
if err != nil {
    return err
}
runErr := rt.Run(ctx, func(run *sdk.Context) error {
    if err := run.Step("inspect", inspect); err != nil {
        return err
    }
    if _, err := run.Checkpoint(); err != nil {
        return err
    }
    return run.Step("report", writeReport)
})
return errors.Join(runErr, rt.Close())
```

Here `sdk` is an alias for `github.com/Chao-superw/durarun-go/runtime`, and `errors` is from the standard library. Use stable, distinct IDs for work you want the SDK to track. Persist the state later steps need in the runner workspace. The callback starts from its entry point after a restart, and `Step` skips work recorded as complete in the restored checkpoint. Code outside `Step` runs again, including the checkpoint call in this sketch. The bundled example avoids taking that checkpoint a second time during recovery.

## Try process interruption and recovery

Build both programs from the checkout on macOS or Linux:

```sh
go build -o ./bin/durarun-local ./cmd/durarun-local
go build -o ./bin/sdk-agent ./examples/sdk-agent
SDK_EXAMPLE_PAUSE=1 ./bin/durarun-local run -state ./agent-run -- ./bin/sdk-agent "inspect source"
```

Press Ctrl+C after `checkpoint committed`, then run:

```sh
./bin/durarun-local resume -state ./agent-run
```

Inspect `report.md` in the workspace printed by the runner. The first attempt writes `notes.txt`, and the resumed attempt reads the restored notes and writes the report without printing `inspecting input` again. Use a fresh state directory for each new run, keep the executable available at its original path, and pass absolute paths for external inputs.

## Recovery contract

- A checkpoint captures workspace files and recorded step completion. State held in memory needs to be saved explicitly by the Agent.
- Steps absent from the restored checkpoint may run again, including a step whose external call completed before its progress was checkpointed. Use application-level idempotency for operations such as sending messages, creating tickets or writing to a remote service.
- The local runner reports the process exit status and workspace path.
- The [example](../examples/sdk-agent/main.go) uses deterministic local work. Integrating an independent Go Agent with real model and tool calls is a separate validation step.

Run `go test ./...` for the SDK and local runner regressions.
