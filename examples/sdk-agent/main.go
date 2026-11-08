package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	sdk "github.com/Chao-superw/durarun-go/runtime"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: sdk-agent INPUT")
	}
	rt, err := sdk.New(sdk.Config{Durable: true})
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close() }()
	return rt.Run(context.Background(), func(agent *sdk.Context) error {
		if err := agent.Step("inspect", func(context.Context) error {
			fmt.Println("inspecting input")
			words := strings.Fields(os.Args[1])
			return os.WriteFile("notes.txt", []byte(strings.Join(words, "\n")), 0600)
		}); err != nil {
			return err
		}
		if os.Getenv("DURARUN_RECOVERY") != "true" {
			if _, err := agent.Checkpoint(); err != nil {
				return err
			}
			fmt.Println("checkpoint committed")
			if os.Getenv("SDK_EXAMPLE_PAUSE") == "1" {
				time.Sleep(30 * time.Second)
			}
		}
		return agent.Step("report", func(context.Context) error {
			notes, err := os.ReadFile("notes.txt")
			if err != nil {
				return err
			}
			return os.WriteFile("report.md", []byte("# Inspection\n\n"+string(notes)+"\n"), 0600)
		})
	})
}
