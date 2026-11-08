//go:build linux || darwin

package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	runner "github.com/Chao-superw/durarun-go/internal/localrunner"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 || (args[0] != "run" && args[0] != "resume") {
		fmt.Fprintln(os.Stderr, "Usage: durarun-local run -state DIR -- PROGRAM [ARGS...]\n       durarun-local resume -state DIR")
		return 2
	}
	resume := args[0] == "resume"
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	state := flags.String("state", "", "private run directory")
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if *state == "" || resume == (flags.NArg() > 0) {
		fmt.Fprintln(os.Stderr, "run requires -state and a program; resume requires -state only")
		return 2
	}
	ctx, stop := runner.SignalContext(context.Background())
	defer stop()
	result, work, err := runner.RunLocal(ctx, *state, flags.Args(), resume)
	if work != "" {
		fmt.Fprintln(os.Stderr, "workspace:", work)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return result.ExitCode
}
