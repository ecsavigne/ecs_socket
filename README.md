# ecs_socket
Library for send data way websocket

### 1. Install lib
        go get -u github.com/ecsavigne/ecs_socket
### 2. Update version
    1. Go to github and copy info of last commit 
    2. Do it:
            go get -u github.com/ecsavigne/ecs_socket@<last-commit>
### 3. Code in server
    // Eje. Gin file main.go

    package main

    import (
        "github.com/gin-gonic/gin"
        "net/http"
        "github.com/ecsavigne/ecs_socket/server"
	    "github.com/ecsavigne/ecs_socket/socket_type"
    )

    var srv *server.Server

    func main() {
        // Crear el router
        router := gin.Default()

        // Ruta GET simple
        router.GET("/ws", func(c *gin.Context) {
          srv = server.NewServer(socket_type.SConfig{
                W: g.Writer,
                R: g.Request,
            })

            srv.Listen()
        })

        // Route send data for client
        router.Get("receiveData", func(c *gin.Context){
            msg := map[string]any{
                "Data": "EveryThing",
            }

            srv.SendMessage(msg)
        })

        router.Run(":8080")
    }

### 4. Code client
        // Client.go

        func main() {
            cl := client.NewClient(socket_type.CConfig{
                Url: "exemplo.server.com/ws",
                Tls: true,
            })
            if cl.Error != nil {
                log.Fatal(err)
            }

            for {
                cl.ReceiveMessage()

                msg := make(map[string]any)
                json.Unmarshal(cl.GetLastMessage(), &msg)
                content = msg["Data"].(string)

                if content != "" {
                    fmt.Println("\nPrint every thing: ", content)
                    break
                } else {
                    fmt.Println("'content' not found")
                    return
                }
            }
        }