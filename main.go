// velenza-trellis (task-graph) is the Layer 3 Velenza Arbor Stack plan-to-done MCP server.
//
// Subcommands:
//
//	velenza-trellis mcp                       run the Streamable HTTP MCP service
//	velenza-trellis init                      apply bootstrap/schema_v1.sql to the experimental DB
//	velenza-trellis reset --confirm           drop + recreate the experimental DB
//	velenza-trellis fg-validator-runner FILE  run the read-only FG checks over frozen snapshot JSON
//	velenza-trellis arbor-info [--config FILE] emit the Layer 3 Velenza Arbor Stack JSON descriptor
//
// See README.md and REQUIREMENTS.md for the three-tier framing, the
// experimental-bootstrap operating mode, and the cutover discipline.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	code, err := runCLI(ctx, os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "task-graph:", err)
	}
	if code != 0 {
		os.Exit(code)
	}
}

func runCLI(ctx context.Context, args []string) (int, error) {
	if len(args) < 2 {
		usage()
		return 2, nil
	}
	var err error
	switch args[1] {
	case "mcp":
		err = cmdMCP(ctx, args[2:])
	case "init":
		err = cmdInit(ctx, args[2:])
	case "reset":
		err = cmdReset(ctx, args[2:])
	case "fg-validator-runner":
		err = cmdFGValidatorRunner(ctx, args[2:], os.Stdout)
	case "arbor-info":
		code, arborErr := runArborInfoCLI(ctx, args[2:], os.Stdout)
		if errors.Is(arborErr, context.Canceled) {
			return 0, nil
		}
		return code, arborErr
	case "help", "-h", "--help":
		usage()
		return 0, nil
	default:
		usage()
		return 2, nil
	}

	if errors.Is(err, context.Canceled) {
		return 0, nil
	}
	if err != nil {
		return 1, err
	}
	return 0, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, usageText())
}
