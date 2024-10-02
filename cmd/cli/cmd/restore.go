/*
Copyright © 2024 Stephen Wan <stephen@stephenwan.net>
*/
package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/stephen/litefs-backup/cmd/cli/cmd/client"
	"github.com/superfly/ltx"
)

// restoreCmd represents the restore command
var restoreCmd = &cobra.Command{
	Use:     "restore",
	Short:   "restore a database to a timestamp or txid",
	Aliases: []string{"r"},
	Args:    cobra.ExactArgs(1),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		timestamp, _ := cmd.Flags().GetString("timestamp")
		txID, _ := cmd.Flags().GetString("txid")

		if txID != "" {
			if _, err := ltx.ParseTXID(txID); err != nil {
				return err
			}
		}

		if timestamp != "" {
			if _, err := ltx.ParseTimestamp(timestamp); err != nil {
				return err
			}
		}

		if txID != "" && timestamp != "" {
			return fmt.Errorf("cannot specify both --txid and --timestamp flags")
		} else if txID == "" && timestamp == "" {
			return fmt.Errorf("must specify one of --txid and --timestamp flags")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		ts, _ := cmd.Flags().GetString("timestamp")
		timestamp, _ := ltx.ParseTimestamp(ts)
		txID, _ := cmd.Flags().GetString("txid")

		opt := client.RestoreByTXID(txID)
		if txID == "" {
			opt = client.RestoreByTimestamp(timestamp)
		}

		db := args[0]

		if check, _ := cmd.Flags().GetBool("check"); check {
			result, err := Client().CheckRestoreDatabase(cmd.Context(), db, opt)
			if err != nil {
				return err
			}

			VerticalTable(os.Stdout, "db restore check", [][]string{{db, result.Timestamp.Format(time.RFC3339), result.TXID.String()}}, "database", "timestamp", "pos")
		} else {
			newPos, err := Client().RestoreDatabase(cmd.Context(), db, opt)
			if err != nil {
				return err
			}

			VerticalTable(os.Stdout, "db restored", [][]string{{db, newPos.String()}}, "database", "pos")
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)

	restoreCmd.Flags().String("timestamp", "", "timestamp to restore to (or earlier)")
	restoreCmd.Flags().String("txid", "", "txid to restore to")
	restoreCmd.Flags().Bool("check", false, "check (dry-run) mode to verify a restore point")
}
