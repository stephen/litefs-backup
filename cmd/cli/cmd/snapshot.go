package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// snapshotCmd represents the snapshot command
var snapshotCmd = &cobra.Command{
	Use:     "snapshot",
	Short:   "download the current snapshot of the given database",
	Aliases: []string{"s"},
	Args:    cobra.ExactArgs(1),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if format, _ := cmd.Flags().GetString("format"); format != "ltx" && format != "sqlite" {
			return fmt.Errorf("invalid format: %s", format)
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		format, _ := cmd.Flags().GetString("format")

		r, err := Client().ExportDatabase(cmd.Context(), args[0], format)
		if err != nil {
			return err
		}

		var out io.WriteCloser = os.Stdout
		if output, _ := cmd.Flags().GetString("output"); output != "" {
			out, err = os.Create(output)
			if err != nil {
				return err
			}

			defer out.Close()
		}

		if _, err := io.Copy(out, r); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(snapshotCmd)

	snapshotCmd.Flags().StringP("output", "o", "", "output file")
	snapshotCmd.Flags().StringP("format", "f", "sqlite", "format: sqlite (default) or ltx")
}
