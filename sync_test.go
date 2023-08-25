package dynasc

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TypedSyncMapTestSuite struct {
	suite.Suite
}

type Test interface {
	Test()
}

type TestFunc func(t *testing.T)

func (fn TestFunc) Test(t *testing.T) {
	fn(t)
}

func (s *TypedSyncMapTestSuite) TestStore() {
	// Define test cases.
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Normal: the key does not exist in the sync map",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				k, v := "key", "value"
				m.Store(k, v)
				assert.Equal(t, v, func() string {
					actual, _ := m.Load(k)
					return actual
				}())
			},
		},
		{
			name: "Normal: the key exists in the sync map",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				k, v := "key", "value"
				m.Store(k, "")
				m.Store(k, v)
				assert.Equal(t, v, func() string {
					actual, _ := m.Load(k)
					return actual
				}())
			},
		},
		{
			name: "Normal: the keys does not exist in the sync map and are stored in parallel",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				kv := map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				}
				wg := &sync.WaitGroup{}
				for k, v := range kv {
					wg.Add(1)
					go func(k, v string) {
						m.Store(k, v)
						wg.Done()
					}(k, v)
				}
				wg.Wait()
				for k, v := range kv {
					assert.Equal(t, v, func() string {
						actual, _ := m.Load(k)
						return actual
					}())
				}
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), tc.test)
	}
}

func (s *TypedSyncMapTestSuite) TestDelete() {
	// Define test cases.
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Normal: the key exists in the sync map",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				k, v := "key", "value"
				m.Store(k, v)
				m.Delete(k)
				assert.Equal(t, "", func() string {
					actual, _ := m.Load(k)
					return actual
				}())
			},
		},
		{
			name: "Normal: the key does not exist in the sync map",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				k := "key"
				m.Delete(k)
				assert.Equal(t, "", func() string {
					actual, _ := m.Load(k)
					return actual
				}())
			},
		},
		{
			name: "Normal: the keys exist in the sync map and are deleted in parallel",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				kv := map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				}
				for k, v := range kv {
					m.Store(k, v)
				}
				wg := &sync.WaitGroup{}
				for k, v := range kv {
					wg.Add(1)
					go func(k, v string) {
						m.Delete(k)
						wg.Done()
					}(k, v)
				}
				wg.Wait()
				for k, _ := range kv {
					assert.Equal(t, "", func() string {
						actual, _ := m.Load(k)
						return actual
					}())
				}
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), tc.test)
	}
}

func (s *TypedSyncMapTestSuite) TestLoad() {
	// Define test cases.
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Normal: the key exists in the sync map",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				k, v := "key", "value"
				m.Store(k, v)
				actual, ok := m.Load(k)
				assert.Equal(t, v, actual)
				assert.Equal(t, true, ok)
			},
		},
		{
			name: "Normal: the key does not exist in the sync map",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				actual, ok := m.Load("key")
				assert.Equal(t, "", actual)
				assert.Equal(t, false, ok)
			},
		},
		{
			name: "Normal: the keys exist in the sync map and are loaded in parallel",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				kv := map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				}
				for k, v := range kv {
					m.Store(k, v)
				}
				wg := &sync.WaitGroup{}
				for k, v := range kv {
					wg.Add(1)
					go func(k, v string) {
						actual, ok := m.Load(k)
						assert.Equal(t, v, actual)
						assert.Equal(t, true, ok)
						wg.Done()
					}(k, v)
				}
				wg.Wait()
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), tc.test)
	}
}

func (s *TypedSyncMapTestSuite) TestRange() {
	// Define test cases.
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Normal: the keys exist in the sync map",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				kv := map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				}
				for k, v := range kv {
					m.Store(k, v)
				}
				m.Range(func(key, value string) bool {
					assert.Contains(t, kv, key)
					assert.Equal(t, kv[key], value)
					return true
				})
			},
		},
		{
			name: "Normal: no key exists in the sync map",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				m.Range(func(key, value string) bool {
					t.Fatal("this function must not be called")
					return true
				})
			},
		},
		{
			name: "Normal: the keys exist in the sync map and range called in parallel",
			test: func(t *testing.T) {
				m := TypedSyncMap[string, string]{}
				kv := map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				}
				for k, v := range kv {
					m.Store(k, v)
				}
				wg := &sync.WaitGroup{}
				for range make([]struct{}, 3) {
					wg.Add(1)
					go func() {
						m.Range(func(key, value string) bool {
							assert.Contains(t, kv, key)
							assert.Equal(t, kv[key], value)
							return true
						})
						wg.Done()
					}()
				}
				wg.Wait()
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), tc.test)
	}
}

func TestTypedSyncMapTestSuite(t *testing.T) {
	// Execute.
	suite.Run(t, &TypedSyncMapTestSuite{})
}
