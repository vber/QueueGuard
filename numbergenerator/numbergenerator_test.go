/*
	go test -bench=.
*/

package numbergenerator

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestReadRecords(t *testing.T) {
	// Setup
	basePath := "test_data"
	ng := NewNumberGenerator(basePath)
	defer os.RemoveAll(basePath) // Clean up after the test

	// Prepopulate with data
	for i := 0; i < 10000; i++ {
		_, err := ng.AppendRecord("primary", 0)
		if err != nil {
			t.Fatalf("Preparation failed: %v", err)
		}
	}

	// Execute
	start := time.Now()
	for i := uint64(1); i <= 10000; i++ {
		_, err := ng.GetStatus("primary", i)
		if err != nil {
			t.Errorf("Failed to get status for record %d: %v", i, err)
		}
	}
	duration := time.Since(start)

	// Report
	t.Logf("Read 10000 records in %v", duration)
}

func TestUpdateRecords(t *testing.T) {
	// Setup
	basePath := "test_data"
	ng := NewNumberGenerator(basePath)
	defer os.RemoveAll(basePath) // Clean up after the test

	// Prepopulate with data
	for i := 0; i < 10000; i++ {
		_, err := ng.AppendRecord("primary", 0)
		if err != nil {
			t.Fatalf("Preparation failed: %v", err)
		}
	}

	// Generate random numbers for update
	rand.Seed(time.Now().UnixNano())
	numbers := make([]uint64, 1000) // update 1000 records
	for i := range numbers {
		numbers[i] = uint64(rand.Intn(10000) + 1)
	}

	// Execute
	start := time.Now()
	err := ng.UpdateStatuses("primary", numbers)
	if err != nil {
		t.Fatalf("Failed to update statuses: %v", err)
	}
	duration := time.Since(start)

	// Report
	t.Logf("Updated 1000 records in %v", duration)
}

func BenchmarkUpdateRecords(b *testing.B) {
	// Setup
	basePath := "bench_data"
	ng := NewNumberGenerator(basePath)
	defer os.RemoveAll(basePath) // Clean up after the benchmark

	// Prepopulate with data
	for i := 0; i < 10000; i++ {
		_, err := ng.AppendRecord("primary", 0)
		if err != nil {
			b.Fatalf("Preparation failed: %v", err)
		}
	}

	// Generate random numbers for update
	rand.Seed(time.Now().UnixNano())
	numbers := make([]uint64, 1000) // Update 1000 records
	for i := range numbers {
		numbers[i] = uint64(rand.Intn(10000) + 1)
	}

	// Execute
	b.ResetTimer() // Start the timer here to only measure the update time
	for i := 0; i < b.N; i++ {
		err := ng.UpdateStatuses("primary", numbers)
		if err != nil {
			b.Fatalf("Failed to update statuses: %v", err)
		}
	}
}
