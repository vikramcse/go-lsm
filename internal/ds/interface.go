package ds

// MemTableImpl is the interface that defines the operations required for the MemTable
type MemTableImpl interface {
	Set(key string, value interface{})
	Get(key string) (interface{}, bool)
	Len() int64
}
