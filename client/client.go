package client

import (
	"encoding/json"
	"fmt"

	"github.com/ecsavigne/ecs_socket/socket_type"
	"github.com/gorilla/websocket"
)

type Client struct {
	connect         *websocket.Conn
	lastMessageType int
	lastMessage     []byte
	Error           error
}

// NewClient returns a new client if there is no error creating of client if there is an error it returns one
// with a nil connection and the error en the Error field
func NewClient(c socket_type.CConfig) *Client {
	if c.Url == "" {
		return &Client{
			Error: fmt.Errorf("Empty Url"),
		}
	}

	if c.Tls {
		c.Url = "wss://" + c.Url
	} else {
		c.Url = "ws://" + c.Url
	}

	conn, _, err := websocket.DefaultDialer.Dial(c.Url, nil)
	if err != nil {
		return &Client{
			Error: err,
		}
	}

	return &Client{
		connect: conn,
		Error:   nil,
	}
}

func (c *Client) SendMessage(data map[string]any) {
	msg, _ := json.Marshal(data)
	c.Error = c.connect.WriteMessage(websocket.BinaryMessage, []byte(msg))
}

func (c *Client) ReceiveMessage() {
	messageType, message, err := c.connect.ReadMessage()
	if err != nil {
		c.Error = err
	}

	c.lastMessageType, c.lastMessage = messageType, message
}

func (c *Client) GetLastMessage() []byte {
	return c.lastMessage
}

func (c *Client) GetLastMessageType() int {
	return c.lastMessageType
}

func (c *Client) Close() {
	c.connect.Close()
}

func (c *Client) Listen() {
	for {
		c.ReceiveMessage()
	}
}
