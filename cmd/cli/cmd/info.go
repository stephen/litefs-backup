/*
Copyright © 2024 Stephen Wan <stephen@stephenwan.net>
*/
package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

// infoCmd represents the info command
var infoCmd = &cobra.Command{
	Use:     "info",
	Short:   "fetch min and max restorable timestamps for given db",
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
			info.MaxTimestamp.Format(time.RFC3339Nano),
		}}

		VerticalTable(os.Stdout, "db info", rows, "database", "min", "max")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
