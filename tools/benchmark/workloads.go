package main

import (
	"fmt"
	"math/rand"
)

// WorkloadPreset defines a YCSB-style workload configuration.
type WorkloadPreset struct {
	Name        string
	Description string
	ReadRatio   float64
	UpdateRatio float64
	InsertRatio float64
	ScanRatio   float64
	RMWRatio    float64 // Read-Modify-Write
}

// YCSB workload presets for industry-standard benchmarking.
var YCSBWorkloads = map[string]WorkloadPreset{
	"A": {
		Name:        "Workload A",
		Description: "Update heavy (50% read, 50% update) - Session store",
		ReadRatio:   0.50,
		UpdateRatio: 0.50,
	},
	"B": {
		Name:        "Workload B",
		Description: "Read mostly (95% read, 5% update) - Photo tagging",
		ReadRatio:   0.95,
		UpdateRatio: 0.05,
	},
	"C": {
		Name:        "Workload C",
		Description: "Read only (100% read) - User profile cache",
		ReadRatio:   1.0,
	},
	"D": {
		Name:        "Workload D",
		Description: "Read latest (95% read, 5% insert) - User status updates",
		ReadRatio:   0.95,
		InsertRatio: 0.05,
	},
	"E": {
		Name:        "Workload E",
		Description: "Short ranges (95% scan, 5% insert) - Threaded conversations",
		ScanRatio:   0.95,
		InsertRatio: 0.05,
	},
	"F": {
		Name:        "Workload F",
		Description: "Read-modify-write (50% read, 50% RMW) - User database",
		ReadRatio:   0.50,
		RMWRatio:    0.50,
	},
}

// workloadExecutor executes operations based on workload preset.
type workloadExecutor struct {
	preset    WorkloadPreset
	client    *kvClient
	keyRange  int
	valueSize int
	scanLimit int
	nextKey   int // For insert operations
}

func newWorkloadExecutor(preset WorkloadPreset, client *kvClient, keyRange, valueSize, scanLimit int) *workloadExecutor {
	return &workloadExecutor{
		preset:    preset,
		client:    client,
		keyRange:  keyRange,
		valueSize: valueSize,
		scanLimit: scanLimit,
		nextKey:   keyRange, // Start inserts after existing keys
	}
}

// executeOp performs one operation based on workload distribution.
func (we *workloadExecutor) executeOp() error {
	r := rand.Float64()
	cumulative := 0.0

	// Read operation
	cumulative += we.preset.ReadRatio
	if r < cumulative {
		_, err := we.client.rawGet(makeKey(rand.Intn(we.keyRange)))
		return err
	}

	// Update operation (put to existing key)
	cumulative += we.preset.UpdateRatio
	if r < cumulative {
		return we.client.rawPut(makeKey(rand.Intn(we.keyRange)), makeValue(we.valueSize))
	}

	// Insert operation (put to new key)
	cumulative += we.preset.InsertRatio
	if r < cumulative {
		key := we.nextKey
		we.nextKey++
		return we.client.rawPut(makeKey(key), makeValue(we.valueSize))
	}

	// Scan operation
	cumulative += we.preset.ScanRatio
	if r < cumulative {
		return we.client.rawScan(makeKey(rand.Intn(we.keyRange)), uint32(we.scanLimit))
	}

	// Read-Modify-Write operation
	cumulative += we.preset.RMWRatio
	if r < cumulative {
		key := makeKey(rand.Intn(we.keyRange))
		_, err := we.client.rawGet(key)
		if err != nil {
			return err
		}
		return we.client.rawPut(key, makeValue(we.valueSize))
	}

	return fmt.Errorf("workload distribution error: no operation selected")
}
