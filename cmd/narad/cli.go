package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/debanganthakuria/narad/internal/storage"
)

func runCLI(args []string) error {
	fs := flag.NewFlagSet("cli", flag.ContinueOnError)
	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintln(out, "Usage: narad cli [flags]")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Interactive REPL over a single append-only log file. Useful for")
		fmt.Fprintln(out, "exercising the storage layer without spinning up the HTTP server.")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Flags:")
		fs.PrintDefaults()
	}

	logPath := fs.String("log", "log.data", "path to the log file")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	l, err := storage.NewLog(*logPath)
	if err != nil {
		return fmt.Errorf("open log %s: %w", *logPath, err)
	}
	defer l.Close()

	reader := bufio.NewReader(os.Stdin)

	fmt.Println("narad cli")
	fmt.Println("commands: produce <message> | consume <offset|latest> | exit")

	for {
		fmt.Print("> ")

		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("read error:", err)
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "exit" {
			fmt.Println("bye")
			return nil
		}
		handleCLICommand(l, line)
	}
}

func handleCLICommand(l *storage.Log, input string) {
	parts := strings.Fields(input)

	switch parts[0] {
	case "produce":
		if len(parts) < 2 {
			fmt.Println("usage: produce <message>")
			return
		}
		msg := strings.Join(parts[1:], " ")
		offset, err := l.Append([]byte(msg))
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Println("offset:", offset)

	case "consume":
		if len(parts) < 2 {
			fmt.Println("usage: consume <offset|latest>")
			return
		}
		var offset int64
		if parts[1] == "latest" {
			offset = l.LatestOffset()
		} else {
			o, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Println("invalid offset")
				return
			}
			offset = o
		}
		msg, err := l.Read(offset)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Println(string(msg))

	default:
		fmt.Println("unknown command:", parts[0])
	}
}
