/*
Copyright © 2024 Stephen Wan <stephen@stephenwan.net>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:     "import",
	Aliases: []string{"i"},
	Short:   "import a database into the litefs cluster",
	Long: `Import a database into the litefs cluster.

If the database already exists in the cluster, the imported
file will overwrite the existing data and increment the txid
for that database.
`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		database, fileName := args[0], args[1]

		f, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer f.Close()

		txid, err := Client().ImportDatabase(cmd.Context(), database, f)
		if err != nil {
			return err
		}

		VerticalTable(os.Stdout, "db imported", [][]string{{database, txid.String()}}, "database", "txid")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
}
