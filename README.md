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
#### // Con class 
			
			import { ref } from 'vue'

			interface OfficialEvent {
			  event_name: string
			  data: any
			}
			
			export type Callback = (msg: OfficialEvent | null, err?: Error) => void
			
			export interface WSInterface {
			  receive: (handlerCallback: Callback) => void
			  close: () => void // Añadido para cerrar explícitamente la conexión.
			}
			
			export class ClientWebSocket implements WSInterface {
			  private _wsUrl = ''
			  protected _ws = ref<WebSocket|null>(null)
			  private callback = ref<Callback>((_ = {} as OfficialEvent) => {
			    // noop
			  })
			
			  protected retry = ref(0)
			
			  public constructor (wsUrl: string) {
			    this._wsUrl = wsUrl
			  }
			
			  // methods:
			  sendCallback = (ws: WebSocket, cb: Callback) => {
			    ws.onmessage = (msg: MessageEvent) => {
			      // console.log('Message receive: ', msg)
			      const evtMsg = msg.data
			      // console.log({ cb, evtMsg })
			      if (cb && evtMsg) {
			        cb(evtMsg)
			      } else {
			        console.log('No Envia a callback: ')
			      }
			    }
			  }
			
			  public receive = (handlerCallback: Callback) => {
			    this.callback.value = handlerCallback
			    this._ws.value = new WebSocket(this._wsUrl)
			
			    if (!this._ws.value) { return }
			
			    this._ws.value.onopen = () => {
			      console.log('Conected!')
			      this.retry.value = 0
			    }
			
			    // error
			    try {
			      this._ws.value.onerror = (e) => {
			        if (this.retry.value < 10) {
			          this.retry.value++
			          this.receive(this.callback.value)
			        } else {
			          const target = e.target as WebSocket
			          this.callback.value?.(null, new Error(`readyState: ${target.readyState}, ws_url: ${target.url}, retry: ${this.retry.value}`))
			        }
			      }
			    } catch (e) {
			      console.error(e)
			    }
			
			    // Send msg to callback
			    this.sendCallback(this._ws.value, this.callback.value)
			
			    this._ws.value.onclose = () => {
			      this.callback.value?.(null, new Error(`Disconnected!!!! ... Retry: ${this.retry.value}`))
			
			      setTimeout(() => {
			        console.log(`Connection retry: ${this.retry.value}`)
			        this.retry.value++
			        this.receive(this.callback.value)
			      }, 3000)
			    }
			  }
			
			  public close = () => {
			    this._ws.value?.close()
			  }
			}
			
			export type { OfficialEvent }

			
#### Ó	

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
#### use in TypeScript or JavaScript
			// codigo dentro de un store	
			import { ClientWebSocket, type WSInterface } from 'src/components/whatsapp/official/types/ws-class-type'
			const clientNotice = ref<WSInterface>()
			// code in a function init (ex: listener)
				clientNotice.value = new ClientWebSocket('wss://oficial.crmsocialhub.com.br/wsNotice')

			// initilize ws how you wish : useWsStore().listener()
			// listener ws events : 
				const ws = useWsStore()
				const eventOfficial = ref({})

				function initWS () {
				 // init, connect and receive from websocket
				  ws.clientNotice.receive((message, err) => {
					// console.log('Message ws receive: ', message)
					if (err) {
					  console.log('Occurred an error in ws: ', err.message)
					  return
					}
				
					// const data = JSON.parse(message)
					eventOfficial.value = JSON.parse(message)
				  })
				}
					
				onMounted(() => {
				  initWS()
				})

				onUnmounted(() => {
				  ws.clientWS.close()
				})
				
