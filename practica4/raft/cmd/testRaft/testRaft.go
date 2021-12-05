package main

import (
	"fmt"
	"golang.org/x/crypto/ssh"
	"net"
    "os"
    "os/user"
    "io/ioutil"
    "time"
    "net/rpc"
    "raft/internal/raft"
    "raft/internal/comun/rpctimeout"
)

const (
	//hosts
	MAQUINA_LOCAL = "155.210.154.192"
	MAQUINA1      = "155.210.154.193"
	MAQUINA2      = "155.210.154.194"
	MAQUINA3      = "155.210.154.198"

	//puertos
	PUERTOLOCAL = "29630"
	PUERTOREPLICA1 = "29631"
	PUERTOREPLICA2 = "29632"
	PUERTOREPLICA3 = "29633"

	//nodos replicas
	LOCAL = MAQUINA_LOCAL + ":" + PUERTOLOCAL
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

    CLIENTE = MAQUINA_LOCAL

    LISTA_IPS1 = LOCAL
    LISTA_IPS2 = LOCAL + " " + REPLICA1
    LISTA_IPS3 = LOCAL + " " + REPLICA1 + " " + REPLICA2
    LISTA_IPS4 = LOCAL + " " + REPLICA1 + " " + REPLICA2 + " " + REPLICA3
    LISTA_IPS5 = REPLICA1 + " " + REPLICA2 + " " + REPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA="/home/a796919/ssdd/practicas/practica4/raft/cmd/srvraft/./main"

  EXECLIENTE="/home/a796919/ssdd/practicas/practica4/raft/cmd/clientraft/./main"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001
)

var NUM_NODOS = 1

func conectar(CONN_TYPE string, CONN_HOST string ) (*ssh.Session, error){

    usr, err := user.Current()
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to get user %v", err.Error())
        os.Exit(1)
    }

    fichero := usr.HomeDir + "/.ssh/id_rsa"
    key, err := ioutil.ReadFile(fichero)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to read id_rsa %v", err.Error())
        os.Exit(1)
    }

    firma, err := ssh.ParsePrivateKey(key)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to get sign %v", err.Error())
        os.Exit(1)
    }

    config := &ssh.ClientConfig{
            User: "a796919",
            Auth: []ssh.AuthMethod{
                ssh.PublicKeys(firma),
            },
            HostKeyCallback: func(hostname string, 
              remote net.Addr, key ssh.PublicKey) error {
                return nil
            },
    }

    cliente, err := ssh.Dial(CONN_TYPE,CONN_HOST + ":" + "22", config)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to dial %v", err.Error())
        os.Exit(1)
    }

    sesion, err := cliente.NewSession()
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to create session %v", err.Error())
        os.Exit(1)
    }
    return sesion, nil
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func obtenerIdLider() int{

    var reply raft.RepBool
    client, err := rpc.Dial("tcp", LOCAL)

    if err != nil {
        fmt.Println("Error al conectarse.")
    }

    defer client.Close()

    err = rpctimeout.CallTimeout(client,
      "NodoRaft.ObtenerEstadoRPC",
      raft.Vacio{}, &reply, 50 * time.Millisecond)
    checkError(err)

    if(reply.Value == true){
        return 0
    } else {
        client, err := rpc.Dial("tcp", REPLICA1)

        if err != nil {
            fmt.Println("Error al conectarse.")
        }

        defer client.Close()

        err = rpctimeout.CallTimeout(client,
          "NodoRaft.ObtenerEstadoRPC",
          raft.Vacio{}, &reply, 50 * time.Millisecond)
        checkError(err)
        if(reply.Value == true){
            return 1
        } else {
            client, err := rpc.Dial("tcp", REPLICA2)

            if err != nil {
                fmt.Println("Error al conectarse.")
            }

            defer client.Close()

            err = rpctimeout.CallTimeout(client,
              "NodoRaft.ObtenerEstadoRPC",
              raft.Vacio{}, &reply, 50 * time.Millisecond)
            checkError(err)
            if(reply.Value == true){
                return 2
            } else {
                client, err := rpc.Dial("tcp", REPLICA3)

                if err != nil {
                    fmt.Println("Error al conectarse.")
                }

                defer client.Close()

                err = rpctimeout.CallTimeout(client,
                  "NodoRaft.ObtenerEstadoRPC",
                  raft.Vacio{}, &reply, 50 * time.Millisecond)
                checkError(err)
                 if(reply.Value == true){
                    return 3
                } else {
                    return -1
                }
            }
        } 
    }

}

func iniciarCliente(ip string, host string){

    sesionServidor, err := conectar("tcp", ip)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to connect to server %v", err.Error())
        os.Exit(1)
    }
    defer sesionServidor.Close()

    err = sesionServidor.Run(EXECLIENTE + " " + host)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to run server %v", err.Error())
        os.Exit(1)
    }

}


func iniciarMaquina(ip string, id string, lista string){

	sesionServidor, err := conectar("tcp", ip)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to connect to server %v", err.Error())
        os.Exit(1)
    }
    defer sesionServidor.Close()

    err = sesionServidor.Run(EXECREPLICA + " " + id + " " + lista)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Failed to run server %v", err.Error())
        os.Exit(1)
    }

}

func apagarMaquina(ip string){
    var reply raft.Vacio
    client, err := rpc.Dial("tcp", ip)

    if err != nil {
        fmt.Println("Error al conectarse.")
    }

    defer client.Close()

    err = rpctimeout.CallTimeout(client,
      "NodoRaft.Para",raft.Vacio{}, &reply, 50 * time.Millisecond)
    checkError(err)

    fmt.Println("--> Se ha parado el nodo " + ip)

}

func T1PruebaInicioParada(){
	
	go iniciarMaquina(MAQUINA_LOCAL,"0",LISTA_IPS1)

	time.Sleep(10 * time.Second)	//esperamos un tiempo

	apagarMaquina(LOCAL)

    fmt.Println("--> Test finalizado sin errores. Comprobar LOG file")
}

func T2PruebaEleccion1Lider(){
    
    go iniciarMaquina(MAQUINA_LOCAL,"0",LISTA_IPS3)
    go iniciarMaquina(MAQUINA1,"1",LISTA_IPS3)
    go iniciarMaquina(MAQUINA2,"2",LISTA_IPS3)
    

    time.Sleep(10 * time.Second)    //esperamos un tiempo

    apagarMaquina(LOCAL)
    apagarMaquina(REPLICA1)
    apagarMaquina(REPLICA2)

    fmt.Println("--> Test finalizado sin errores. Comprobar LOG file")
}

func T3PruebaEleccion2Lider(){
    
    go iniciarMaquina(MAQUINA_LOCAL,"0",LISTA_IPS4)
    go iniciarMaquina(MAQUINA1,"1",LISTA_IPS4)
    go iniciarMaquina(MAQUINA2,"2",LISTA_IPS4)
    go iniciarMaquina(MAQUINA3,"3",LISTA_IPS4)
    

    time.Sleep(10 * time.Second)    //esperamos un tiempo

    switch obtenerIdLider() {
    case 0:
        apagarMaquina(LOCAL)
        fmt.Println("--> Apagando lider. Maquina local")
        time.Sleep(10 * time.Second)
        apagarMaquina(REPLICA1)
        apagarMaquina(REPLICA2)
        apagarMaquina(REPLICA3)
    case 1:
        apagarMaquina(REPLICA1)
        fmt.Println("--> Apagando lider. Maquina replica1")
        time.Sleep(10 * time.Second)
        apagarMaquina(LOCAL)
        apagarMaquina(REPLICA2)
        apagarMaquina(REPLICA3)
    case 2:
        apagarMaquina(REPLICA2)
         fmt.Println("--> Apagando lider. Maquina replica2")
        time.Sleep(10 * time.Second)
        apagarMaquina(LOCAL)
        apagarMaquina(REPLICA1)
        apagarMaquina(REPLICA3)
    case 3:
        apagarMaquina(REPLICA3)
         fmt.Println("--> Apagando lider. Maquina replica3")
        time.Sleep(10 * time.Second)
        apagarMaquina(LOCAL)
        apagarMaquina(REPLICA1)
        apagarMaquina(REPLICA2)
    default:
        fmt.Fprintf(os.Stderr, "No se ha podido obtener lider.")
        apagarMaquina(LOCAL)
        apagarMaquina(REPLICA1)
        apagarMaquina(REPLICA2)
        apagarMaquina(REPLICA3)
        return
    }

    fmt.Println("--> Test finalizado sin errores. Comprobar LOG file")
}

func T4Comprometer3Entradas(){
    
    go iniciarMaquina(MAQUINA1,"0",LISTA_IPS5)
    go iniciarMaquina(MAQUINA2,"1",LISTA_IPS5)
    go iniciarMaquina(MAQUINA3,"2",LISTA_IPS5)
    
    time.Sleep(10 * time.Second)    //esperamos un tiempo

    go iniciarCliente(CLIENTE,REPLICA1)
    fmt.Println("--> Lanzado cliente")
    time.Sleep(1 * time.Second)
    go iniciarCliente(CLIENTE,REPLICA1)
    fmt.Println("--> Lanzado cliente")
    time.Sleep(1 * time.Second)
    go iniciarCliente(CLIENTE,REPLICA1)
    fmt.Println("--> Lanzado cliente")

    time.Sleep(5 * time.Second) 

    apagarMaquina(REPLICA1)
    apagarMaquina(REPLICA2)
    apagarMaquina(REPLICA3)

    fmt.Println("--> Test finalizado sin errores. Comprobar LOG file")
}

func main() {

	fmt.Println("--------------- TEST INICIADO ---------------")

    //T1PruebaInicioParada()
	//T2PruebaEleccion1Lider()
    //T3PruebaEleccion2Lider()
    T4Comprometer3Entradas()

	fmt.Println("--------------- TEST SUPERADO ---------------")


}