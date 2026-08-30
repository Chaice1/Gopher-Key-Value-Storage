package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkStorage_Set(b *testing.B) {
	s := NewStorage()

	val := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key", val)
	}
}

func TestStorage_FullFlow(t *testing.T) {
	s := NewStorage()

	test := struct {
		Key   string
		Value []byte
	}{
		Key:   "test",
		Value: []byte("test"),
	}

	_, err := s.Get(test.Key)
	require.Error(t, err)

	s.Set(test.Key, test.Value)

	val, err := s.Get(test.Key)
	require.NoError(t, err)
	assert.Equal(t, test.Value, val)

	err = s.Delete(test.Key)
	require.NoError(t, err)

	err = s.Delete(test.Key)
	require.NoError(t, err)
}

func TestStorage_Concurrency(t *testing.T) {
	s := NewStorage()

	wg := &sync.WaitGroup{}

	numWorkers := 100

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			s.Set("test", []byte("test"))
			_, _ = s.Get("test")
			_ = s.Delete("test")
		}()
	}

	wg.Wait()
	assert.True(t, true)
}
