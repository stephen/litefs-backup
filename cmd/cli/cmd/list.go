package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// listCmd represents the cluster command
var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"l"},
	Short:   "Get databases and positions for this cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		pos, err := Client().Pos(cmd.Context())
		if err != nil {
			return err
		}

		var rows [][]string
		for n, p := range pos {
			rows = append(rows, []string{n, p.TXID.String(), p.PostApplyChecksum.String()})
		}

		Table(os.Stdout, rows, "database", "txid", "checksum")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}
