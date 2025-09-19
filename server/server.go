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

type hub struct {
	mutex   sync.RWMutex
	clients map[*websocket.Conn]bool
}

func NewHub() *hub {
	return &hub{
		clients: make(map[*websocket.Conn]bool),
	}
}

func (h *hub) add(conn *websocket.Conn) {
	h.mutex.Lock()
	h.clients[conn] = true
	h.mutex.Unlock()
}

func (h *hub) remove(conn *websocket.Conn) {
	h.mutex.Lock()
	delete(h.clients, conn)
	h.mutex.Unlock()
}

func (h *hub) Broadcast(data any) {
	msg, _ := json.Marshal(data)

	h.mutex.RLock()
	conns := make([]*websocket.Conn, 0, len(h.clients))
	for c := range h.clients {
		conns = append(conns, c)
	}
	h.mutex.RUnlock()

	for _, c := range conns {
		if err := c.WriteMessage(websocket.TextMessage, msg); err != nil {
			h.remove(c)
			c.Close()
		}
	}
}

func (h *hub) Count() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return len(h.clients)
}

type Server struct {
	// Permite la conexión desde cualquier origen
	hub      *hub
	upgrader websocket.Upgrader
	config   socket_type.SConfig
}

func NewServer(c socket_type.SConfig, h *hub) *Server {
	server := &Server{
		config: c,
		hub:    h,
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

	return conn
}

func (s *Server) ReceiveMessage(c *websocket.Conn) (messageType int, p []byte, err error) {
	return c.ReadMessage()
}

// Function Listen
func (s *Server) Listen() {
	conn := s.setHandler(s.config.W, s.config.R)

	s.hub.add(conn)
	defer func() {
		s.hub.remove(conn)
		conn.Close()
	}()

	for {
		_, _, e := s.ReceiveMessage(conn)
		if e != nil {
			break
		}

	}
}
