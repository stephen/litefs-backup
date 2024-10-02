package cmd

import (
	"fmt"

	"github.com/superfly/lfsc-go"
)

func Client() *lfsc.Client {
	client := lfsc.NewClient()
	if url, _ := rootCmd.Flags().GetString("endpoint"); url != "" {
		client.URL = url
	}

	cluster, _ := rootCmd.Flags().GetString("cluster")
	client.Token = fmt.Sprintf("cluster %s", cluster)

	return client
}
