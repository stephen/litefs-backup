package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

// infoCmd represents the info command
var infoCmd = &cobra.Command{
	Use:     "info",
	Short:   "Fetch min and max restorable timestamps for given db",
	Aliases: []string{"i"},
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		info, err := Client().Info(cmd.Context(), args[0])
		if err != nil {
			return err
		}

		rows := [][]string{{
			info.Name,
			info.MinTimestamp.Format(time.RFC3339Nano),
			info.MinTXID,
			info.MaxTimestamp.Format(time.RFC3339Nano),
			info.MaxTXID,
		}}

		VerticalTable(os.Stdout, "db info", rows, "database", "min timestamp", "min txid", "max timestamp", "max txid")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
