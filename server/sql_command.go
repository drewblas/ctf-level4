package server

import (
  "github.com/goraft/raft"
  // "github.com/goraft/raftd/db"
)

func init() {
        raft.RegisterCommand(&SqlCommand{})
}

// This command writes a value to a key.
type SqlCommand struct {
  Query   []byte `json:"query"`
}

// Creates a new write command.
func NewSqlCommand(query []byte) *SqlCommand {
  return &SqlCommand{
    Query: query,
  }
}

// The name of the command in the log.
func (c *SqlCommand) CommandName() string {
  return "sqlcommand"
}

// Writes a value to a key.
func (c *SqlCommand) Apply(server raft.Server) (interface{}, error) {
  ql := server.Context().(*QueryLog)
  state := server.State()
  iface, err := ql.Execute(state, c.Query)
  return iface, err
}
