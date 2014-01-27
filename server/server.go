package server

import (
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/util"
	"time"
	"github.com/goraft/raft"
	// "github.com/goraft/raftd/command"
	// "github.com/goraft/raftd/db"
	"bytes"
	"encoding/json"
	"strings"
)

var allowPassalong = false

type Server struct {
	name       string
	path       string
	listen     string
	router     *mux.Router
	httpServer *http.Server
	client     *transport.Client
	cluster    *Cluster
	raftServer raft.Server
	queryLog   *QueryLog
	connectionString string
}

type Join struct {
	Self ServerAddress `json:"self"`
}

type JoinResponse struct {
	Self    ServerAddress   `json:"self"`
	Members []ServerAddress `json:"members"`
}

type PassalongResponse struct {
	SequenceNumber int `json:"sequence"`
}

// Creates a new server.
func New(path, listen string) (*Server, error) {
	cs, err := transport.Encode(listen)
	if err != nil {
		return nil, err
	}

	// cs := "http://" + listen

	sqlPath := filepath.Join(path, "storage.sql")
	util.EnsureAbsent(sqlPath)

	log.Println("STARTING path:", path, " listen:", listen, " cs: ", cs)

	s := &Server{
		name:    listen,
		path:    path,
		listen:  listen,
		router:  mux.NewRouter(),
		client:  transport.NewClient(),
		cluster: NewCluster(path, cs),
		queryLog:      NewQueryLog(sql.NewSQL(sqlPath)),
		connectionString: cs,
	}

	return s, nil
}

func (s *Server) raftInit(primary string) {
	var err error

	transporter := raft.NewHTTPTransporter("/raft")
	transporter.Transport.Dial = transport.UnixDialer
	// transporter.Transport = &http.Transport{Dial: transport.UnixDialer, DisableKeepAlives: false}
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.queryLog, s.connectionString)
	// func NewServer(name string, path string, transporter Transporter, stateMachine StateMachine, ctx interface{}, connectionString string) (Server, error) {
	
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	if primary != "" {
		// Join to primary if specified.

		log.Println("[Raft] Attempting to join primary:", primary)

		if !s.raftServer.IsLogEmpty() {
			log.Fatal("[Raft] Cannot join with an existing log")
		}
		if err := s.Join(primary); err != nil {
			log.Fatal(err)
		}

	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.Println("[Raft] Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString,
		})
		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Println("[Raft] Recovered from log")
	}
}

// Starts the server.
func (s *Server) ListenAndServe(primary string) error {
	var err error

	s.raftInit(primary)

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}	

	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")
	s.router.HandleFunc("/healthcheck", s.healthcheckHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")
	s.router.HandleFunc("/states", s.statesHandler).Methods("GET")
	s.router.HandleFunc("/passalong", s.passalongHandler).Methods("POST")

	// Start Unix transport
	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}
	return s.httpServer.Serve(l)
}

func (s *Server) statesHandler(w http.ResponseWriter, req *http.Request) {
	r := ""

	r = fmt.Sprintf("Leader: %v, State: %v, Peers: %v", s.raftServer.Leader(), s.raftServer.State(), s.raftServer.Peers())
	w.Write([]byte(r))
}

// Client operations

func (s *Server) healthcheckPrimary() bool {
	_, err := s.client.SafeGet(s.cluster.primary.ConnectionString, "/healthcheck")

	if err != nil {
		log.Printf("The primary appears to be down: %s", err)
		return false
	} else {
		return true
	}
}

// Joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString,
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)

	cs, _ := transport.Encode(leader)
	log.Println("[RAFT SLAVE] Posting to ", cs, " ** ", leader)

	// resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	// resp.Body.Close()
	_, err := s.client.SafePost(cs, "/join", &b)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("[RAFT LEADER] Received Join: ", command.Name, " - ", command.ConnectionString)

	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) passalongHandler(w http.ResponseWriter, req *http.Request) {
	state := s.raftServer.State()
	query, err := ioutil.ReadAll(req.Body)

	log.Debugf("[%s] Received passalong query: %#v", state, string(query))

	i_resp, err := s.raftServer.Do(NewSqlCommand(query))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	executedQuery := i_resp.(*ExecutedQuery)

	log.Debugf("[%s] Returning passalong response to %#v: %#v", state, string(query), executedQuery.SequenceNumber)

	resp := &PassalongResponse{ executedQuery.SequenceNumber }
	b := util.JSONEncode(resp)
	w.Write(b.Bytes())
}


// This is the only user-facing function, and accordingly the body is
// a raw string rather than JSON.
func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
	state := s.raftServer.State()
	query, err := ioutil.ReadAll(req.Body)

	if err != nil {
		log.Printf("Couldn't read body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	log.Debugf( fmt.Sprintf("Leader: %v, State: %v, Peers: %v", s.raftServer.Leader(), s.raftServer.State(), s.raftServer.Peers()) )

	if state != "leader" {
		////////////////   SLAVE
		if allowPassalong {	

			leader := s.raftServer.Leader()
			var cs string
			for name, member := range s.raftServer.Peers() {
				if name == leader {
					cs = member.ConnectionString
				}
			}

			if cs == "" {
				log.Printf("No Leader found!")
				log.Printf( fmt.Sprintf("Leader: %v, State: %v, Peers: %v", s.raftServer.Leader(), s.raftServer.State(), s.raftServer.Peers()) )

				http.Error(w, "No Leader found!", http.StatusInternalServerError)
				return
			}

			body, err := s.client.SafePost(cs, "/passalong", strings.NewReader(string(query)))

			if err != nil {
				log.Printf("Couldn't pass-along query to %v: %s", cs, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			r := &PassalongResponse{}
			if err := util.JSONDecode(body, r); err != nil {
				log.Printf("Invalid passalong response: %s", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			var resp []byte

			for i := 0; i < 5; i++ {
				time.Sleep(100 * time.Millisecond)

				resp = s.queryLog.GetResponse(r.SequenceNumber)

				if resp != nil {
					break
				}

				log.Printf("Still waiting on passalong...")
			}

			log.Debugf("[%s] Returning response to %#v: %#v", state, string(query), string(resp))
			w.Write(resp)
		}else{
			http.Error(w, "Only the primary can service queries, but this is a "+state, http.StatusBadRequest)
			return
		}
	}else{
		////////////////   MASTER / PRIMARY / LEADER
		log.Debugf("[%s] Received query: %#v", state, string(query))

		i_resp, err := s.raftServer.Do(NewSqlCommand(query))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		executedQuery := i_resp.(*ExecutedQuery)
		resp := executedQuery.Response

		log.Debugf("[%s] Returning response to %#v: %#v", state, string(query), string(resp))
		w.Write(resp)
	}
}

func (s *Server) healthcheckHandler(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}