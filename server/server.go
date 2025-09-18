package server

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/ecsavigne/ecs_socket/socket_type"
	"github.com/gorilla/websocket"
)

type Socker interface {
	SendMessage(msg string)
	ReceiveMessage()
}

var clients map[*websocket.Conn]bool = make(map[*websocket.Conn]bool)

type Server struct {
	// Permite la conexión desde cualquier origen
	countConections int
	mutex           sync.RWMutex
	upgrader        websocket.Upgrader
	config          socket_type.SConfig
	// lastMessageType int
	// lastMessage     []byte
	// Error error
}

func NewServer(c socket_type.SConfig) *Server {
	server := &Server{
		countConections: 0,
		config:          c,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	return server
}

func (s *Server) setHandler(w http.ResponseWriter, r *http.Request) *websocket.Conn {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	// Server connect
	s.mutex.Lock()
	clients[conn] = true
	s.mutex.Unlock()
	s.countConections++

	return conn
}

func (s *Server) SendMessage(data any) {
	msg, _ := json.Marshal(data)

	conns := make([]*websocket.Conn, 0, len(clients))
	s.mutex.Lock()
	for c := range clients {
		conns = append(conns, c)
	}
	s.mutex.Unlock()

	for _, c := range conns {
		e := c.WriteMessage(websocket.BinaryMessage, msg)
		if e != nil {
			s.mutex.Lock()
			s.Close(c)
			delete(clients, c)
			s.countConections--
			s.mutex.Unlock()
		}
	}
}

func (s *Server) ReceiveMessage(c *websocket.Conn) (messageType int, p []byte, err error) {
	return c.ReadMessage()
}

func (s *Server) Close(c *websocket.Conn) {
	c.Close()
}

func (s *Server) GetCountConections() int {
	return s.countConections
}

// Function Listen
func (s *Server) Listen() {
	conn := s.setHandler(s.config.W, s.config.R)
	defer s.Close(conn)

	for {
		_, _, e := s.ReceiveMessage(conn)
		if e != nil {
			s.mutex.Lock()
			delete(clients, conn)
			s.countConections--
			s.mutex.Unlock()
			break
		}

	}
}
