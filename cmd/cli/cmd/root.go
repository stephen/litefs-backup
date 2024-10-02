package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "lfsb",
	Short: "litefs-backup administration cli",
	Long:  `litefs-backup is a cli for administrating a LiteFS backup server.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if cluster, _ := cmd.Flags().GetString("cluster"); cluster == "" {
			return fmt.Errorf("cluster must be specified")
		}
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringP("cluster", "c", os.Getenv("LFSB_CLUSTER"), "lfsb cluster name")
	rootCmd.PersistentFlags().StringP("endpoint", "e", os.Getenv("LFSB_ENDPOINT"), "lfsb endpoint")
}
