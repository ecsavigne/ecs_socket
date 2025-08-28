package server

import (
	"encoding/json"
	"net/http"

	"github.com/ecsavigne/ecs_socket/socket_type"
	"github.com/gorilla/websocket"
)

type Socker interface {
	SendMessage(msg string)
	ReceiveMessage()
}

type Server struct {
	// Permite la conexión desde cualquier origen
	connect         *websocket.Conn
	upgrader        websocket.Upgrader
	lastMessageType int
	lastMessage     []byte
	Error           error
}

func NewServer(c socket_type.SConfig) *Server {
	server := &Server{
		lastMessageType: websocket.TextMessage,
		lastMessage:     []byte{},
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	server.setHandler(c.W, c.R)

	return server
}

func (s *Server) setHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.Error = err
		return
	}

	// Server connect
	s.connect = conn
}

func (s *Server) SendMessage(data map[string]any) {
	msg, _ := json.Marshal(data)
	s.Error = s.connect.WriteMessage(websocket.BinaryMessage, msg)
}

func (s *Server) ReceiveMessage() {
	messageType, message, err := s.connect.ReadMessage()
	if err != nil {
		s.Error = err
	}

	s.lastMessageType = messageType
	s.lastMessage = message
}

func (s *Server) GetLastMessage() []byte {
	return s.lastMessage
}

func (s *Server) GetLastMessageType() int {
	return s.lastMessageType
}

func (s *Server) Close() {
	s.connect.Close()
}

// Function Listen
func (s *Server) Listen() {
	for {
		s.ReceiveMessage()
		if s.Error != nil {
			break
		}

	}
}
