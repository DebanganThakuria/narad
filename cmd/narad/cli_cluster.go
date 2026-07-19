package main

import (
	"net/http"
	"net/url"

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
	var cancel bool
	cmd := &cobra.Command{
		Use:   "decommission <node-id>",
		Short: "drain a node's partitions off and remove it from the cluster",
		Long: "Marks a node for decommission: the leader sheds every partition it owns\n" +
			"onto the other nodes and, once drained, removes it from the Raft voter set.\n" +
			"Use --cancel to stop an in-progress decommission (the node keeps its\n" +
			"partitions and starts receiving again).",
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			path := "/v1/cluster/members/" + url.PathEscape(args[0]) + "/decommission"
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
	return cmd
}

func clusterMovesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "moves",
		Short: "list partitions currently being moved between nodes",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return cliClient().getAndPrint("/v1/cluster/moves")
		},
	}
}

func clusterMembersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "members",
		Short: "list cluster members with partition counts and drain status",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return cliClient().getAndPrint("/v1/cluster/members")
		},
	}
}
