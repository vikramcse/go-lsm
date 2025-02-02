package golsm

import (
	"testing"

	"github.com/vikramcse/go-lsm/internal/ds"
)

func TestMemtableSkipList(t *testing.T) {
	skipListDS := ds.NewSkipListMemTable()
	mt := NewMemTable(skipListDS)

	testCases := []struct {
		key   string
		value []byte
	}{
		{"key1", []byte("value1")},
		{"key2", []byte("value2")},
		{"key3", []byte("value3")},
	}

	// Test Put
	for _, tc := range testCases {
		mt.Put(tc.key, tc.value)
	}

	for _, tc := range testCases {
		value, ok := mt.Get(tc.key)
		if !ok {
			t.Errorf("Key %s not found", tc.key)
		}
		if string(value) != string(tc.value) {
			t.Errorf("Expected value %s, got %s", string(tc.value), string(value))
		}
	}

	// Test non-existent key
	_, exists := mt.Get("nonexistent")
	if exists {
		t.Error("Expected false for non-existent key")
	}

	// Test Size and Len
	if mt.Len() != int64(len(testCases)) {
		t.Errorf("Expected length %d, got %d", len(testCases), mt.Len())
	}
}

func TestMemtableRBT(t *testing.T) {
	rblDS := ds.NewRedBlackTreeMemTable()
	mt := NewMemTable(rblDS)

	testCases := []struct {
		key   string
		value []byte
	}{
		{"key1", []byte("value1")},
		{"key2", []byte("value2")},
		{"key3", []byte("value3")},
	}

	// Test Put
	for _, tc := range testCases {
		mt.Put(tc.key, tc.value)
	}

	for _, tc := range testCases {
		value, ok := mt.Get(tc.key)
		if !ok {
			t.Errorf("Key %s not found", tc.key)
		}
		if string(value) != string(tc.value) {
			t.Errorf("Expected value %s, got %s", string(tc.value), string(value))
		}
	}

	// Test non-existent key
	_, exists := mt.Get("nonexistent")
	if exists {
		t.Error("Expected false for non-existent key")
	}

	// Test Size and Len
	if mt.Len() != int64(len(testCases)) {
		t.Errorf("Expected length %d, got %d", len(testCases), mt.Len())
	}
}
