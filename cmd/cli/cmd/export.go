package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// exportCmd represents the export command
var exportCmd = &cobra.Command{
	Use:     "export",
	Short:   "export and download the database at its current position",
	Aliases: []string{"e"},
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
	rootCmd.AddCommand(exportCmd)

	exportCmd.Flags().StringP("output", "o", "", "output file")
	exportCmd.Flags().StringP("format", "f", "sqlite", "format: sqlite (default) or ltx")
}
