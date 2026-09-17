package server

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/dracory/uid"
	"github.com/ecsavigne/ecs_socket/socket_type"
	"github.com/gorilla/websocket"
)

type tuple map[string]any

type Socker interface {
	SendMessage(msg string)
	ReceiveMessage()
}

type connClient struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (self *connClient) send(data []byte) error {
	self.writeMu.Lock()
	defer self.writeMu.Unlock()

	return self.conn.WriteMessage(websocket.TextMessage, data)
}

type hub struct {
	mutex   sync.RWMutex
	clients map[string]*connClient
	// clients map[*connClient]bool
}

// NewHub returns a new hub for managing connections to the server
func NewHub() *hub {
	return &hub{
		clients: make(map[string]*connClient),
		// clients: make(map[*websocket.Conn]bool),
	}
}

//	func (h *hub) add(conn *websocket.Conn) {
//		h.mutex.Lock()
//		defer h.mutex.Unlock()
//		h.clients[conn] = true
//	}
func (h *hub) add(conn *websocket.Conn, key ...string) (*connClient, string) {
	c := &connClient{conn: conn}
	h.mutex.Lock()
	defer h.mutex.Unlock()

	k := uid.UuidV7(true)
	if len(key) > 0 {
		k = key[0]
	}

	h.clients[k] = c

	return c, k
}

//	func (h *hub) remove(conn *websocket.Conn) {
//		h.mutex.Lock()
//		defer h.mutex.Unlock()
//		delete(h.clients, conn)
//	}
func (h *hub) remove(key string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	delete(h.clients, key)
}

// Broadcast sends a message to all connected clients.
// If a client connection is broken, it will be removed from the hub.
// The message is marshalled to JSON before being sent.
func (h *hub) Broadcast(data any) {
	msg, e := json.Marshal(data)
	if e != nil {
		return
	}

	h.mutex.RLock()
	conns := make([]tuple, 0, len(h.clients))
	for key, c := range h.clients {
		conns = append(conns, tuple{"conn": c, "key": key})
	}
	h.mutex.RUnlock()

	for _, t := range conns {
		c := t["conn"].(*connClient)
		key := t["key"].(string)
		if err := c.send(msg); err != nil {
			h.remove(key)
			c.conn.Close()
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

// Send data to one client use when hub is not global if hub is global send to all clients with hub.Broadcast(msg)
/*  example:
/ 	srv = server.NewServer(socket_type.SConfig{
		W: g.Writer,
		R: g.Request,
	},server.NewHub())
 srv.Listen()
 /* // Send data to one client
  * srv.SendMessage(data any)
  *
 The message is marshalled to JSON before being sent.*/
func (s *Server) SendMessage(data any) {
	s.hub.Broadcast(data)
}

/*
Create one conection, add to the hub of server and listen for send messages
*/
func (s *Server) Listen(key ...string) {
	conn := s.setHandler(s.config.W, s.config.R)
	if conn == nil {
		return
	}
	defer conn.Close()

	connCl, k := s.hub.add(conn, key...)
	defer func() {
		s.hub.remove(k)
		connCl.conn.Close()
	}()

	for {
		_, _, e := s.ReceiveMessage(conn)
		if e != nil {
			break
		}

	}
}
