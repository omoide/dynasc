package dynasc

import "sync"

type TypedSyncMap[T, U any] sync.Map

// Store sets the value for a key.
func (m *TypedSyncMap[T, U]) Store(key T, value U) {
	(*sync.Map)(m).Store(key, value)
}

// Delete deletes the value for a key.
func (m *TypedSyncMap[T, U]) Delete(key T) {
	(*sync.Map)(m).Delete(key)
}

// Load returns the value stored in the map for a key, or zero value if no value is present.
// The ok result indicates whether value was found in the map.
func (m *TypedSyncMap[T, U]) Load(key string) (value U, ok bool) {
	if value, ok := (*sync.Map)(m).Load(key); ok {
		return value.(U), true
	}
	return value, false
}

// Range calls fn sequentially for each key and value present in the map.
// If the fn returns false, range stops the iteration.
func (m *TypedSyncMap[T, U]) Range(fn func(key T, value U) bool) {
	(*sync.Map)(m).Range(func(key, value any) bool {
		return fn(key.(T), value.(U))
	})
}
