package cmd

import (
	"fmt"

	"github.com/stephen/litefs-backup/cmd/cli/cmd/client"
)

func Client() *client.Client {
	client := client.NewClient()
	if url, _ := rootCmd.Flags().GetString("endpoint"); url != "" {
		client.URL = url
	}

	cluster, _ := rootCmd.Flags().GetString("cluster")
	client.Token = fmt.Sprintf("cluster %s", cluster)

	return client
}
