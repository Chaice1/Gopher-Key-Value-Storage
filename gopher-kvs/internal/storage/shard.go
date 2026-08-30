package storage

import "sync"

type Shard struct {
	mu *sync.RWMutex
	m  map[string][]byte
}

func (s *Shard) Set(key string, value []byte) {
	s.mu.Lock()
	s.m[key] = value
	s.mu.Unlock()
}

func (s *Shard) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, ok := s.m[key]; ok {
		return val, nil
	}
	return nil, ErrorNotFound
}

func (s *Shard) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil

}
