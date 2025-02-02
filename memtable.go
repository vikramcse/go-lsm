package golsm

import (
	"sync"

	"github.com/vikramcse/go-lsm/internal/ds"
)

type MemTable struct {
	data ds.MemTableImpl
	mu   sync.RWMutex // this is for thread safety
	size int64        // track the size of the memtable
}

// NewMemTable creates and initializes a new MemTable
func NewMemTable(ds ds.MemTableImpl) *MemTable {
	return &MemTable{
		data: ds,
		size: 0,
	}
}

// Put adds or updates a key-value pair in the MemTable
func (m *MemTable) Put(key string, value []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// If key exists, subtract its value length from size
	if existing, ok := m.data.Get(key); ok {
		m.size -= int64(len(existing.([]byte)))
	}

	m.data.Set(key, value)
	m.size += int64(len(value))
}

// Get retrieves a value for a given key from the MemTable
func (m *MemTable) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, ok := m.data.Get(key)
	if !ok {
		return nil, false
	}

	return value.([]byte), true
}

// Size returns the current size of the MemTable in bytes
func (m *MemTable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Len returns the number of entries in the MemTable
func (m *MemTable) Len() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(m.data.Len())
}
