package storage

import "sync"

const countShards = 256

type Storage struct {
	shards [countShards]*Shard
}

func NewStorage() *Storage {

	shards := [countShards]*Shard{}
	for i := 0; i < countShards; i++ {
		shards[i] = &Shard{
			mu: &sync.RWMutex{},
			m:  make(map[string][]byte),
		}
	}
	return &Storage{
		shards: shards,
	}
}

func (s *Storage) GetShard(str string) *Shard {
	h := Hash(str)
	return s.shards[(h & 255)]
}

func (s *Storage) Set(key string, value []byte) {
	shard := s.GetShard(key)
	shard.Set(key, value)
}
func (s *Storage) Get(key string) ([]byte, error) {
	shard := s.GetShard(key)
	return shard.Get(key)
}

func (s *Storage) Delete(key string) error {
	shard := s.GetShard(key)
	return shard.Delete(key)
}
