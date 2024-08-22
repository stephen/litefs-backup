package httputil

import "context"

type clusterNameContextKey struct{}

// ContextWithOrgID returns a context with the given cluster name.
func ContextWithClusterName(ctx context.Context, clusterName string) context.Context {
	return context.WithValue(ctx, clusterNameContextKey{}, clusterName)
}

// ClusterNameFromContext returns the cluster name from a context. It panics
// if context doesn't have one.
func ClusterNameFromContext(ctx context.Context) string {
	return ctx.Value(clusterNameContextKey{}).(string)
}
