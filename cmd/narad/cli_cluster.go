package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/spf13/cobra"
)

// newClusterCmd groups the operator commands for partition rebalance and
// decommission: draining a node, and inspecting in-flight moves and
// per-member placement. Rebalance itself is automatic (the leader triggers
// it on membership change), so there is no "rebalance" verb — these commands
// drive decommission and observe the result.
func newClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "inspect and drive partition placement (rebalance, decommission)",
	}
	cmd.AddCommand(clusterDecommissionCmd(), clusterMovesCmd(), clusterMembersCmd())
	return cmd
}

func clusterDecommissionCmd() *cobra.Command {
	var cancel, dryRun bool
	cmd := &cobra.Command{
		Use:   "decommission <node-id>",
		Short: "drain a node's partitions off and remove it from the cluster",
		Long: "Marks a node for decommission: the leader sheds every partition it owns\n" +
			"onto the other nodes and, once drained, removes it from the Raft voter set.\n" +
			"A decommission that could never complete safely is refused with the reasons.\n" +
			"Use --dry-run to see the verdict without changing anything, and --cancel to\n" +
			"stop an in-progress decommission (the node keeps its partitions and starts\n" +
			"receiving again).",
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			path := "/v1/cluster/members/" + url.PathEscape(args[0]) + "/decommission"
			switch {
			case cancel && dryRun:
				return errors.New("--dry-run and --cancel cannot be combined")
			case dryRun:
				return cliClient().postAndPrint(path+"?dry_run=true", nil)
			}
			method := http.MethodPost
			if cancel {
				method = http.MethodDelete
			}
			resp, err := cliClient().do(method, path, nil)
			if err != nil {
				return err
			}
			resp.Body.Close()
			return nil
		},
	}
	cmd.Flags().BoolVar(&cancel, "cancel", false, "cancel an in-progress decommission")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "report whether the node could be decommissioned, and why not, without changing anything")
	return cmd
}

func clusterMovesCmd() *cobra.Command {
	var detail bool
	cmd := &cobra.Command{
		Use:   "moves",
		Short: "list partitions currently being moved between nodes",
		Long: "Lists every in-flight partition move with each side's liveness and why a\n" +
			"move is blocked. --detail also asks each destination for its move worker's\n" +
			"own report. Abort a move with: narad cluster moves abort <topic> <partition>.",
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return cliClient().getAndPrint(withDetail("/v1/cluster/moves", detail))
		},
	}
	cmd.Flags().BoolVar(&detail, "detail", false, "also ask each destination for its move worker's state")
	cmd.AddCommand(clusterMovesAbortCmd())
	return cmd
}

func clusterMovesAbortCmd() *cobra.Command {
	var target string
	cmd := &cobra.Command{
		Use:   "abort <topic> <partition>",
		Short: "abort an in-flight partition move; the partition stays with its owner",
		Long: "Clears the move's target, so the partition stays with its current owner and\n" +
			"the destination discards its copy. With --target, the abort is refused when\n" +
			"the move now targets another node. The leader may plan a move for the\n" +
			"partition again later.",
		Args: cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			if _, err := strconv.Atoi(args[1]); err != nil {
				return fmt.Errorf("partition must be a number, got %q", args[1])
			}
			path := "/v1/cluster/moves/" + url.PathEscape(args[0]) + "/" + url.PathEscape(args[1]) + "/abort"
			if target != "" {
				path += "?target=" + url.QueryEscape(target)
			}
			return cliClient().postAndPrint(path, nil)
		},
	}
	cmd.Flags().StringVar(&target, "target", "", "the destination node the move must still target")
	return cmd
}

func clusterMembersCmd() *cobra.Command {
	var detail bool
	cmd := &cobra.Command{
		Use:   "members",
		Short: "list cluster members with partition counts, Raft role and drain status",
		Long: "Lists every member with its partition counts, voter and leader flags,\n" +
			"heartbeat age and, for a draining node, why its decommission is blocked.\n" +
			"--detail also asks every member for its own status: dispatch backlog,\n" +
			"quarantined partition copies and move workers.",
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return cliClient().getAndPrint(withDetail("/v1/cluster/members", detail))
		},
	}
	cmd.Flags().BoolVar(&detail, "detail", false, "also ask every member for its own status")
	cmd.AddCommand(clusterMembersForgetCmd())
	return cmd
}

// withDetail adds ?detail=true to path when detail is set.
func withDetail(path string, detail bool) string {
	if detail {
		return path + "?detail=true"
	}
	return path
}

func clusterMembersForgetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "forget <node-id>",
		Short: "remove a Raft server that has no member record",
		Long: "Removes a Raft voter or non-voter that has no member record, such as a\n" +
			"joiner admitted by a 3.0.x leader that never registered. It moves and\n" +
			"deletes no data: a server with a member record is refused (decommission\n" +
			"it instead), and so is one a partition assignment names.",
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return cliClient().postAndPrint("/v1/cluster/members/"+url.PathEscape(args[0])+"/forget", nil)
		},
	}
}
