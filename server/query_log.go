package server

import (
  "sync"
)

// The log of all queries
type QueryLog struct {
  sequenceNumber int
  queries  map[int][]byte
  responses map[int][]byte
  mutex sync.RWMutex
}

// Creates a new database.
func NewQueryLog() *QueryLog {
  return &QueryLog{
    sequenceNumber: 0,
    queries: make(map[int][]byte),
    responses: make(map[int][]byte),
  }
}

// Retrieves the value for a given key.
func (ql *QueryLog) GetResponse(idx int) []byte {
  ql.mutex.RLock()
  defer ql.mutex.RUnlock()
  return ql.responses[idx]
}

// Sets the value for a given key.
func (ql *QueryLog) Record(sql []byte) int {
  ql.mutex.Lock()
  defer ql.mutex.Unlock()
  defer func() { ql.sequenceNumber += 1 }()
  ql.queries[ql.sequenceNumber] = sql
  return ql.sequenceNumber
}
