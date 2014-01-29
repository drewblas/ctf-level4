package server

import (
  "sync"
  "fmt"
  "stripe-ctf.com/sqlcluster/log"
  "stripe-ctf.com/sqlcluster/sql"
  "stripe-ctf.com/sqlcluster/util"
  "errors"
)

type ExecutedQuery struct {
  SequenceNumber int
  Query []byte
  Response []byte
}

// The log of all queries
type QueryLog struct {
  sequenceNumber int
  queries  map[int][]byte
  responses map[int][]byte
  sql        *sql.SQL
  mutex sync.RWMutex
}

// Creates a new database.
func NewQueryLog(newsql *sql.SQL) *QueryLog {
  return &QueryLog{
    sequenceNumber: 0,
    queries: make(map[int][]byte),
    responses: make(map[int][]byte),
    sql: newsql,
  }
}

// Retrieves the value for a given key.
func (ql *QueryLog) GetResponse(idx int) []byte {
  ql.mutex.RLock()
  defer ql.mutex.RUnlock()
  return ql.responses[idx]
}

func (ql *QueryLog) Execute(state string, query []byte) (*ExecutedQuery, error) {
  ql.mutex.Lock()
  defer ql.mutex.Unlock()
  defer func() { ql.sequenceNumber += 1 }()
  ql.queries[ql.sequenceNumber] = query

  output, err := ql.sql.Execute(string(query))

  if err != nil {
    var msg string
    if output != nil && len(output.Stderr) > 0 {
      template := `Error executing %#v (%s)

SQLite error: %s`
      msg = fmt.Sprintf(template, query, err.Error(), util.FmtOutput(output.Stderr))
    } else {
      msg = err.Error()
    }

    return nil, errors.New(msg)
  }

  formatted := fmt.Sprintf("SequenceNumber: %d\n%s",
    ql.sequenceNumber, output.Stdout)

  if state == "leader" || log.Verbose() {
    log.Printf("[%s] [%d] Executed %#v - Response %#v", state, ql.sequenceNumber, string(query), formatted)
  }

  ql.responses[ql.sequenceNumber] = []byte(formatted)

  return &ExecutedQuery{ql.sequenceNumber, query, []byte(formatted)}, nil
}
