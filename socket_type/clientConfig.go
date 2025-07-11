package socket_type

// CConfig is the configuration of the client
type CConfig struct {
	Url string // url of connection where server is running "localhost:8080" tls = 0
	Tls bool   // true si es una conexión segura (wss://) false si es una conexión insegura (ws://)
}
