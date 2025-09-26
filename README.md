# ecs_socket
Library for send data way websocket

### 1. Install lib
        go get -u github.com/ecsavigne/ecs_socket
### 2. Update version
    1. Go to github and copy info of last commit 
    2. Do it:
            go get -u github.com/ecsavigne/ecs_socket@<last-commit>
### 3. Code in server
####  // Eje. Gin file main.go send message to one client

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
            }, server.NewHub())

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


####  // Eje. Gin file main.go send message to broadcast client

    package main

    import (
        "github.com/gin-gonic/gin"
        "net/http"
        "github.com/ecsavigne/ecs_socket/server"
	    "github.com/ecsavigne/ecs_socket/socket_type"
    )

    var srv *server.Server
    var hubGlobal  = server.NewHub()

    func main() {
        // Crear el router
        router := gin.Default()

        // Ruta GET simple
        router.GET("/ws", func(c *gin.Context) {
            srv := server.NewServer(socket_type.SConfig{
                W: w,
                R: r,
            }, hubGlobal)

            srv.Listen()
        })

        // Route send data for client
        router.Get("receiveData", func(c *gin.Context){
            msg := map[string]any{
                "Data": "EveryThing",
            }

            hubGlobal.Broadcast(msg)
        })

        router.Run(":8080")
    }

### 4. Code client
#### Client.go
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


#### Client typeScript
 			type Callback = (msg: string, err?: Error) => void
            const _ws = WebSocket|null
            const callback = (msg: string, err?: Error) => void
			const retry = ref(0)

   			 const send = (ws: WebSocket, cb: Callback) => {
				ws.onmessage = async (msg: MessageEvent) => {
				  	let text = ''
	  
					if (typeof msg.data === 'string') {
						// caso más común: server manda texto/JSON
						text = msg.data
						console.log('parsed string:')
					} else if (msg.data instanceof Blob) {
						// caso server manda blob
						text = await msg.data.text()
						console.log('parsed blob:')
					} else if (msg.data instanceof ArrayBuffer) {
						// caso binario
						text = new TextDecoder().decode(msg.data)
						console.log('parsed ArrayBuffer:')
					} else {
						text = String(msg.data)
						console.log('parsed unknown:')
					}

				  if (cb && text !== '') {
					cb(text)
				  }
				}
			  }

            const receive = (handlerCallback: (msg: string, err?: Error) => void) => {
                callback = handlerCallback
                _ws = new WebSocket('wss://[server_path]/ws')

                if (!_ws) { return }

                _ws.onopen = () => console.log('Conected!')

                // error
                try {
                _ws.onerror = (e) => {
					if (retry.value < 10) {
			          retry.value++
			          receive(callback.value)
			        } else {
					  console.log(e)
					  const target = e.target as WebSocket
					  callback?.('', new Error(`readyState: ${target.readyState}, ws_url: ${target.url}`))
					}
                }
                } catch (e) {
                console.error(e)
                }

                // Receive msg and send dat for callback
				send(_ws.value, callback.value)

				// handler retry of conections
			   _ws.value.onclose = () => {
			      callback.value?.('', new Error(`Disconnected!!!! ... Retry: ${retry.value}`))
			
			      console.log('Prepared Initial Set timeout')
		 			// try each 3s reconnect 
			      setTimeout(() => {
			        console.log('Set timeout')
			        retry.value++
			        receive(callback.value)
			      }, 3000)
			      console.log('Prepared End Set timeout')
			    }
            }

            wsStore.receive((message, err) => {
            if (err) {
                console.log('Occurred an error in ws: ', err.message)
            }

            console.log('Process message:', message)
            })
