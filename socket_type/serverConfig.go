package socket_type

import "net/http"

// SConfig is the configuration of the server
type SConfig struct {
	W http.ResponseWriter
	R *http.Request
}
