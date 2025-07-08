package server

import (
	"encoding/json"
	"net/http"

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

func NewServer() *Server {
	return &Server{
		lastMessageType: websocket.TextMessage,
		lastMessage:     []byte{},
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.Error = err
		conn.Close()
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
