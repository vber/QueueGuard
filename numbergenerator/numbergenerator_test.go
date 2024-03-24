/*
	go test -bench=.
*/

package numbergenerator

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkAppendRecord(b *testing.B) {
	// Setup - create a temporary directory for testing
	dir, err := os.MkdirTemp("", "numbergen")

	if err != nil {
		b.Fatalf("Could not create temporary directory: %v", err)
	}
	defer os.RemoveAll(dir) // clean up
	b.Log("Temporary directory:", dir)

	// Initialize the NumberGenerator with the temp directory
	gen := NewNumberGenerator(dir)

	// Pre-create a primary key directory to simulate a typical usage scenario
	primaryKey := "test"
	pkDir := filepath.Join(dir, primaryKey)
	if err := os.MkdirAll(pkDir, 0755); err != nil {
		b.Fatalf("Could not create primary key directory: %v", err)
	}

	// Benchmark the AppendRecord function
	b.ResetTimer()
	// b.Log("Benchmarking AppendRecord for", b.N)
	for i := 0; i < b.N; i++ {
		_, err := gen.AppendRecord(primaryKey, 0)
		if err != nil {
			b.Fatalf("AppendRecord failed: %v", err)
		}
	}
	b.StopTimer()

	// Clean up
	gen.CloseAllFiles()
}
