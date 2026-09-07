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

			interface OfficialEvent {
			  event_name: string
			  data: any
			}
			
			export type Callback = (msg: OfficialEvent | null, err?: Error) => void
			
			export interface WSInterface {
			  onReceive: (handlerCallback: Callback) => void
			  close: () => void // Añadido para cerrar explícitamente la conexión.
			}
			
			export class ClientWebSocket implements WSInterface {
			  private readonly wsUrl: string
			  private ws: WebSocket | null = null
			  // eslint-disable-next-line @typescript-eslint/no-empty-function
			  private callback: Callback = () => {}
			
			  private retry = 0
			  private readonly maxRetries = 10
			
			  private reconnectTimer:
			    | ReturnType<typeof setTimeout>
			    | null = null
			
			  private manuallyClosed = false
			
			  public constructor (wsUrl: string) {
			    this.wsUrl = wsUrl
			  }
			
			  public onReceive (handlerCallback: Callback): void {
			    this.callback = handlerCallback
			    this.manuallyClosed = false
			    this.connect()
			  }
			
			  private connect (): void {
			    if (
			      this.ws?.readyState === WebSocket.OPEN ||
			      this.ws?.readyState === WebSocket.CONNECTING
			    ) {
			      return
			    }
			
			    const socket = new WebSocket(this.wsUrl)
			    this.ws = socket
			
			    socket.onopen = () => {
			      console.log('WebSocket connected')
			      this.retry = 0
			    }
			
			    socket.onmessage = (event: MessageEvent) => {
			      let message: OfficialEvent = {} as OfficialEvent
			      try {
			        console.log('Message WebSocket:', event.data)
			        message =
			          typeof event.data === 'string'
			            ? (JSON.parse(event.data) as OfficialEvent)
			            : (event.data as OfficialEvent)
			      } catch (error) {
			        this.callback(
			          null,
			          error instanceof Error
			            ? error
			            : new Error('Message WebSocket invalid')
			        )
			      }
			
			      try {
			        this.callback(message)
			      } catch (error) {
			        console.error('Error processing message in callback:', error)
			        this.callback(
			          null,
			          error instanceof Error
			            ? error
			            : new Error('Message WebSocket invalid')
			        )
			      }
			    }
			
			    socket.onerror = (event) => {
			      console.error('WebSocket error:', event)
			    }
			
			    socket.onclose = (event) => {
			      if (this.ws !== socket) {
			        return
			      }
			
			      this.ws = null
			
			      console.warn('WebSocket cerrado:', {
			        code: event.code,
			        reason: event.reason,
			        wasClean: event.wasClean
			      })
			
			      if (this.manuallyClosed) {
			        return
			      }
			
			      this.callback(
			        null,
			        new Error(
			          `WebSocket desconectado: ${event.code}`
			        )
			      )
			
			      this.scheduleReconnect()
			    }
			  }
			
			  private scheduleReconnect (): void {
			    if (this.reconnectTimer) {
			      return
			    }
			
			    if (this.retry >= this.maxRetries) {
			      this.callback(
			        null,
			        new Error(
			          `Máximo de ${this.maxRetries} reintentos alcanzado`
			        )
			      )
			
			      return
			    }
			
			    const delay = Math.min(
			      1000 * 2 ** this.retry,
			      30_000
			    )
			
			    this.retry += 1
			
			    console.log(
			      `Reconexión ${this.retry}/${this.maxRetries} en ${delay} ms`
			    )
			
			    this.reconnectTimer = setTimeout(() => {
			      this.reconnectTimer = null
			      this.connect()
			    }, delay)
			  }
			
			  public close (): void {
			    this.manuallyClosed = true
			
			    if (this.reconnectTimer) {
			      clearTimeout(this.reconnectTimer)
			      this.reconnectTimer = null
			    }
			
			    const socket = this.ws
			    this.ws = null
			
			    if (socket) {
			      socket.onclose = null
			      socket.close(1000, 'Cierre solicitado')
			    }
			  }
			}

			export type { OfficialEvent }


// Pattern singleton use store
	import { markRaw, ref, shallowRef } from 'vue'
		import { defineStore } from 'pinia'
		import { ClientWebSocket, type WSInterface } from 'src/components/whatsapp/official/types/ws-class-type'
		
		export const useWsStore = defineStore('ws-store', () => {
		  // state
		  const clientWS = ref<WSInterface>()
		  const clientNotice = shallowRef<WSInterface>()
		  // getters
		
		  // actions
		
		  const isInitialized = ref(false)
		
		  function init (): void {
		    if (isInitialized.value) {
		      return
		    }
		
		    clientWS.value = markRaw(
		      new ClientWebSocket(
		        'wss://servicex1.socialhub.pro/ws'
		      )
		    )
		
		    clientNotice.value = markRaw(
		      new ClientWebSocket(
		        'wss://oficial.crmsocialhub.com.br/wsNotice'
		      )
		    )
		
		    isInitialized.value = true
		  }
		
		  function closeNotice (): void {
		    clientNotice.value?.close()
		    clientNotice.value = undefined
		    isInitialized.value = false
		  }
		
		  function closeWS (): void {
		    clientNotice.value?.close()
		    clientNotice.value = undefined
		    isInitialized.value = false
		  }
		
		  function closeAll (): void {
		    clientWS.value?.close()
		    clientNotice.value?.close()
		
		    clientWS.value = undefined
		    clientNotice.value = undefined
		    isInitialized.value = false
		  }
		
		  return {
		    // state
		
		    // getters
		
		    // actions
		    clientWS,
		    clientNotice,
		    init,
		    closeNotice,
		    closeWS,
		    closeAll
		  }
		})

// Use client
// Init Globally client Websocket
		import { useWsStore } from 'src/stores/store-ws'
		import { onMounted, onUnmounted } from 'vue'
		const wsStore = useWsStore()
		onMounted(() => {
		  wsStore.init()
		})
		
		onUnmounted(() => {
		  wsStore.closeNotice()
		  wsStore.closeWS()
		})

// Listen message
	ws.clientNotice.onReceive((message, err) => {
    if (err) {
      console.log('Occurred an error in ws: PageConversationsWhatsapp', err.message)
      return
    }

    eventOfficial.value = message
  })
