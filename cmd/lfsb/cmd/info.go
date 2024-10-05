package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// infoCmd represents the info command
var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Fetch restore points for given db",
	Long: `Fetch restore points for given db. By default
this command will only show the earliest and latest restore points. Use
--all to show all restore points.
`,
	Aliases: []string{"i"},
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		all, _ := cmd.Flags().GetBool("all")
		info, err := Client().Info(cmd.Context(), args[0], all)
		if err != nil {
			return err
		}

		var rows [][]string
		for _, p := range info.RestorablePaths {
			rows = append(rows, []string{p.Timestamp.String(), p.MaxTXID, p.PostApplyChecksum})
		}

		Table(os.Stdout, info.Name, rows, "timestamp", "txid", "post-apply checksum")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
	infoCmd.Flags().BoolP("all", "a", false, "show all restore points")
}
