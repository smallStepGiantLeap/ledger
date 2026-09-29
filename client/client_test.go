package client

import (
	"encoding/json"
	"testing"
)

// grpc.NewClient rejects an invalid default service config, so a broken
// ServiceConfig fails here rather than in a caller's service.
func TestDialAcceptsServiceConfig(t *testing.T) {
	var v map[string]any
	if err := json.Unmarshal([]byte(ServiceConfig), &v); err != nil {
		t.Fatalf("ServiceConfig is not JSON: %v", err)
	}
	cc, err := Dial("passthrough:///unused")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	cc.Close()
}

// Retries live in the mesh; a client-side retry policy would multiply them.
func TestNoClientRetryPolicy(t *testing.T) {
	var sc struct {
		MethodConfig []map[string]any `json:"methodConfig"`
	}
	if err := json.Unmarshal([]byte(ServiceConfig), &sc); err != nil {
		t.Fatal(err)
	}
	for _, mc := range sc.MethodConfig {
		if _, ok := mc["retryPolicy"]; ok {
			t.Errorf("methodConfig %v has a retryPolicy", mc["name"])
		}
	}
}
