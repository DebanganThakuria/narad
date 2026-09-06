package main

// The cobra command tree — the modern CLI surface (`narad topic|pub|
// sub|ctx|server ...`). The original `serve`, `client`, and `version`
// subcommands keep their exact hand-rolled behavior via main.go's
// legacy dispatch (their stdout is a compatibility contract asserted by
// the parity tests); stubs here exist only so `narad --help` lists the
// complete surface.

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Connection flags shared by every command that talks to a broker.
var (
	flagServer        string
	flagUser          string
	flagPassword      string
	flagPasswordStdin bool
)

func cliClient() *httpClient {
	c, err := cliConnection()
	if err != nil {
		// Commands call cliClient() inline; a bad --password-stdin is
		// the only failure and is reported the way cobra reports flag
		// errors.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return newContextHTTPClient(c)
}

// cliConnection resolves the connection settings from flags, env and
// the selected context, reading the password from stdin when asked,
// and warns when credentials would travel over plain http off-box.
func cliConnection() (cliContext, error) {
	password, err := resolvePasswordFlags(flagPassword, flagPasswordStdin, "password")
	if err != nil {
		return cliContext{}, err
	}
	c := resolveContext(flagServer, flagUser, password)
	warnPlaintextCredentials(os.Stderr, c)
	return c, nil
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "narad",
		Short:         "Narad — a message broker that respects your weekend",
		Long:          "Narad is a queue-first message broker in a single binary.\nPlain HTTP in, at-least-once out, and nothing to babysit in between.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVarP(&flagServer, "server", "s", "", "broker URL (default: active context, NARAD_ADDR, or "+defaultServer+")")
	root.PersistentFlags().StringVarP(&flagUser, "user", "u", "", "basic auth user (default: active context or NARAD_USER)")
	root.PersistentFlags().StringVarP(&flagPassword, "password", "p", "", "basic auth password (default: active context or NARAD_PASS); prefer --password-stdin or NARAD_PASS, argv is visible in ps and shell history")
	root.PersistentFlags().BoolVar(&flagPasswordStdin, "password-stdin", false, "read the basic auth password from the first line of stdin")

	root.AddCommand(
		newTopicCmd(),
		newPubCmd(),
		newSubCmd(),
		newReplayCmd(),
		newUserCmd(),
		newBenchCmd(),
		newCtxCmd(),
		newClusterCmd(),
		newServerCmd(),
		legacyStub("serve", "run the HTTP API server (default port 7942)", runServe),
		legacyStub("version", "print build version and exit", runVersion),
	)
	return root
}

// legacyStub wraps a pre-cobra command (serve is the container
// entrypoint; its flag parsing stays its own) as a passthrough.
func legacyStub(name, short string, run func([]string) error) *cobra.Command {
	return &cobra.Command{
		Use:                name,
		Short:              short,
		DisableFlagParsing: true,
		RunE:               func(_ *cobra.Command, args []string) error { return run(args) },
	}
}

func runCobra(args []string) error {
	root := newRootCmd()
	root.SetArgs(args)
	return root.Execute()
}

func newCtxCmd() *cobra.Command {
	ctx := &cobra.Command{
		Use:   "ctx",
		Short: "manage named connection contexts (server + credentials)",
	}

	var server, user, password string
	var passwordStdin bool
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "add or update a context",
		Long:  "Add or update a named context. The password is stored IN CLEAR in contexts.json (mode 0600 in a 0700 directory): anyone who obtains the file (backups, dotfile sync) has the credentials. Prefer --password-stdin over --password so the secret never appears in argv.",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			s, err := loadContextStore()
			if err != nil {
				return err
			}
			if server == "" {
				return fmt.Errorf("--server is required")
			}
			password, err := resolvePasswordFlags(password, passwordStdin, "password")
			if err != nil {
				return err
			}
			s.Contexts[args[0]] = cliContext{Server: server, User: user, Password: password}
			if s.Current == "" {
				s.Current = args[0]
			}
			if err := s.save(); err != nil {
				return err
			}
			fmt.Printf("context %q saved (current: %s)\n", args[0], s.Current)
			return nil
		},
	}
	add.Flags().StringVar(&server, "server", "", "broker URL (required)")
	add.Flags().StringVar(&user, "user", "", "basic auth user")
	add.Flags().StringVar(&password, "password", "", "basic auth password (stored in clear, 0600, in your config dir)")
	add.Flags().BoolVar(&passwordStdin, "password-stdin", false, "read the password from the first line of stdin instead of argv")

	sel := &cobra.Command{
		Use:   "select <name>",
		Short: "make a context the default for every command",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			s, err := loadContextStore()
			if err != nil {
				return err
			}
			if _, ok := s.Contexts[args[0]]; !ok {
				return fmt.Errorf("no context %q (narad ctx ls)", args[0])
			}
			s.Current = args[0]
			if err := s.save(); err != nil {
				return err
			}
			fmt.Printf("current context: %s\n", args[0])
			return nil
		},
	}

	ls := &cobra.Command{
		Use:   "ls",
		Short: "list contexts",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			s, err := loadContextStore()
			if err != nil {
				return err
			}
			if len(s.Contexts) == 0 {
				fmt.Println("no contexts (narad ctx add <name> --server URL)")
				return nil
			}
			for _, n := range s.names() {
				c := s.Contexts[n]
				marker := "  "
				if n == s.Current {
					marker = "* "
				}
				user := c.User
				if user == "" {
					user = "-"
				}
				fmt.Printf("%s%-16s %-40s user=%s\n", marker, n, c.Server, user)
			}
			return nil
		},
	}

	rm := &cobra.Command{
		Use:   "rm <name>",
		Short: "remove a context",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			s, err := loadContextStore()
			if err != nil {
				return err
			}
			if _, ok := s.Contexts[args[0]]; !ok {
				return fmt.Errorf("no context %q", args[0])
			}
			delete(s.Contexts, args[0])
			if s.Current == args[0] {
				s.Current = ""
			}
			return s.save()
		},
	}

	ctx.AddCommand(add, sel, ls, rm)
	return ctx
}

// isTTY reports whether stdout is a terminal — gates color and the
// human-oriented chrome so piped output stays clean.
func isTTY() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// dim wraps s in faint ANSI when stdout is a terminal.
func dim(s string) string {
	if !isTTY() {
		return s
	}
	return "\x1b[2m" + s + "\x1b[0m"
}

// bold wraps s in bold ANSI when stdout is a terminal.
func bold(s string) string {
	if !isTTY() {
		return s
	}
	return "\x1b[1m" + s + "\x1b[0m"
}
