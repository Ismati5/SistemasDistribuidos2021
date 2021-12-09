package raft

import (
	"net/rpc"
	"time"
	//"net"
	//"net/http"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"raft/internal/comun/rpctimeout"
)
// Constante para fijar valor entero no inicializado
const IntNOINICIALIZADO = -1

//  false deshabilita por completo los logs de depuracion
// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
const kEnableDebugLogs = false

// Poner a true para logear a stdout en lugar de a fichero
const kLogToStdout = false

// Cambiar esto para salida de logs en un directorio diferente
const kLogOutputDir = "/home/a796919/ssdd/practicas/practica4/raft/logs_raft/"

type Vacio struct{} //struct vacio para args RPC

type RepBool struct { //struct reply para obtenerEstadoRPC
	Value bool
}

type LogRegister struct {
	currentTerm int
	operacion   interface{}
}

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

type NodoRaft struct {
	mux sync.Mutex //Mutex para proteger acceso a state compartido

	Nodos        []string
	yo           int
	state        int //0 seguidor, 1 candidato, 2 lider
	logger       *log.Logger
	enElecciones bool
	log          []LogRegister
	currentTerm  int
	VotedFor     int
	recibido     chan int
	IdLider      int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	NodosDial []*rpc.Client
}

//ELECCIONES

type ArgsPeticionVoto struct {
	Term         int //term
	CandidateId  int //candidateId
	LastLogIndex int //lastLogIndex
	LastLogTerm  int //lastLogTerm
}

type RespuestaPeticionVoto struct {
	Term        int
	VoteGranted bool
}

//SOMETER OPERACIONES
type ArgsAppendEntries struct {
	Term         int
	IdLider      int
	PrevLogIndex int

	PrevLogTerm int
	Entries     interface{}

	LeaderCommit int
}

type RespuestaAppendEntries struct {
	Term    int
	Success bool
}

func (nr *NodoRaft) InicializarCampos(nodos []string, yo int) {

	nr.Nodos = nodos
	nr.yo = yo
	nr.enElecciones = false
	nr.state = 0
	nr.currentTerm = 0 //currentTerm
	nr.VotedFor = IntNOINICIALIZADO
	nr.commitIndex = 0 //commitIndex
	nr.lastApplied = 0 //lastApplied
	nr.IdLider = IntNOINICIALIZADO

	nr.recibido = make(chan int, len(nodos))
	nr.log = make([]LogRegister, 1) //log[]
	nr.NodosDial = make([]*rpc.Client, len(nodos))
	nr.nextIndex = make([]int, len(nodos))  //nextIndex[]
	nr.matchIndex = make([]int, len(nodos)) //matchIndex[]

	for i := 0; i < len(nodos); i++ {
		nr.nextIndex[i] = 1
		nr.matchIndex[i] = 0
	}
}

func (nr *NodoRaft) RecibirMensajes() {

	for {
		if nr.state != 2 { //No es lider

			rand.Seed(time.Now().UnixNano()) //Generador de semilla para timeout aleatorio

			select { //Si no llegan avisos de mensajes en timeout ms se inician elecciones
			case i := <-nr.recibido: //Recibimos avisos de llegada de mensajes

				switch i {
				case 0: //Recibido mensaje de lider
					fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Recibido mensaje de lider.")
					nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + "] Recibido mensaje de lider.")
				case 1: //Recibida peticion de voto
					fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Recibido peticion voto.")
					nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + "] Recibido peticion voto.")
				case 2: //Es lider
					fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Me convierto en lider.")
					nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + "] Me convierto en lider.")
				}
			//Timeout aleatorio de 150-250 ms
			case <-time.After(time.Duration(150+rand.Intn(250-150)) * time.Millisecond):
				//Si no hay elecciones iniciadas ya y no soy candidato ni lider
				if !nr.enElecciones && nr.IdLider != nr.yo && nr.state == 0 {
					go nr.iniciarElecciones()
				}
			}
		} else { //Es lider
			nr.SometerOperacion(nil)          //Enviamos latido
			time.Sleep(50 * time.Millisecond) //Enviamos latido cada 50 ms
		}
	}

}

func NuevoNodo(nodos []string, yo int, canalAplicar chan AplicaOperacion) *NodoRaft {

	var err error
	nr := &NodoRaft{}
	nr.InicializarCampos(nodos, yo)

	if kEnableDebugLogs {
		nombreNodo := nodos[yo]
		logPrefix := fmt.Sprintf("%s ", nombreNodo)
		if kLogToStdout {
			nr.logger = log.New(os.Stdout, nombreNodo,
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err = os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix),
				os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.logger = log.New(logOutputFile, logPrefix,
				log.Lmicroseconds|log.Lshortfile)
		}
	} else {
		nr.logger = log.New(ioutil.Discard, "", 0)
	}

	//Esperamos 1.25s a que se estabilicen todos los nodos
	time.Sleep(1250 * time.Millisecond)

	//Creamos las conexiones con todos los nodos
	for i, nodo := range nr.Nodos {
		if i != nr.yo {
			nr.NodosDial[i], err = rpc.Dial("tcp", nodo)
			fmt.Println("[" + nr.Nodos[nr.yo] + "] Conectado con " + nodo)
			nr.logger.Println("[" + nr.Nodos[nr.yo] + "] Conectado con " + nodo)
		}
	}

	fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Creado NodoRaft.")

	go nr.RecibirMensajes()
	return nr
}

func (nr *NodoRaft) Para(args Vacio, reply *Vacio) error {
	go func() {
		time.Sleep(5 * time.Millisecond)
		os.Exit(0)
	}()
	return nil
}

func (nr *NodoRaft) ObtenerEstado(args Vacio, reply Vacio) (int, int, bool, int) {
	return nr.yo, nr.currentTerm, nr.state == 2, nr.IdLider
}

func (nr *NodoRaft) ObtenerEstadoRPC(args Vacio, reply *RepBool) error {
	_, _, reply.Value, _ = nr.ObtenerEstado(Vacio{}, Vacio{}) //reply.Value = true si es lider
	return nil
}

//SOMETER OPERACIONES

func (nr *NodoRaft) AppendEntries(args *ArgsAppendEntries, reply *RespuestaAppendEntries) error {
	nr.mux.Lock() //Realizamos la función en exclusión mutua
	defer nr.mux.Unlock()

	nr.IdLider = args.IdLider  //Actualizamos nuestro Id_lider al actual en caso de ser nuevo currentTerm
	nr.currentTerm = args.Term //Actualizamos nuestro currentTerm al del lider
	nr.enElecciones = false    //Al llegar mensaje de lider se han acabado elecciones en caso de haberlas
	nr.recibido <- 0           //Recibido mensaje

	if (args.Term < nr.currentTerm) || ((nr.log[args.PrevLogIndex]).currentTerm != args.PrevLogTerm) {
		reply.Success = false
	} else {
		reply.Success = true
		if args.LeaderCommit > nr.commitIndex {
			if args.LeaderCommit < args.PrevLogIndex+1 {
				nr.commitIndex = args.LeaderCommit
			} else {
				nr.commitIndex = args.PrevLogIndex + 1
			}
		}
	}

	if args.Entries != nil { //El mensaje no es un latido
		//Lo introducimos en el LogRegister
		nr.log = append(nr.log, LogRegister{currentTerm: nr.currentTerm, operacion: args.Entries})

		fmt.Println("[Raft "+nr.Nodos[nr.yo]+"] Me ha llegado una operacion de tipo", args.Entries)
		nr.logger.Println("[Raft "+nr.Nodos[nr.yo]+"] Me ha llegado una operacion de tipo", args.Entries)
		nr.logger.Println("[LOG] Estado actual del log:", nr.log)
	} else {
		fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Me ha llegado latido.")
		nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + "] Me ha llegado latido.")
	}

	reply.Term = nr.currentTerm //Le enviamos nuestro currentTerm al lider
	return nil
}

func (nr *NodoRaft) ConsensoSometer(i int, reply []RespuestaAppendEntries, terminado chan bool, respuestasRecibidas int, operacion interface{}) {

	recibido := false

	fmt.Println("[Raft - Lider " + nr.Nodos[nr.yo] + "] Enviado mensaje a : " + nr.Nodos[i])
	nr.logger.Println("[Raft - Lider " + nr.Nodos[nr.yo] + "] Enviado mensaje a : " + nr.Nodos[i])

	//Inicializamos los argumentos
	args := ArgsAppendEntries{
		Term:         nr.currentTerm,
		IdLider:      nr.yo,
		PrevLogIndex: nr.nextIndex[i] - 1,
		PrevLogTerm:  (nr.log[nr.nextIndex[i]-1]).currentTerm,
		Entries:      operacion,
		LeaderCommit: nr.commitIndex,
	}

	//Inicializamos la respuesta
	reply[i] = RespuestaAppendEntries{
		Term:    0,
		Success: false,
	}

	for !recibido { //Bucle infinito hasta que nos responda el nodo i

		fin := nr.NodosDial[i].Go("NodoRaft.AppendEntries", &args, &reply[i], nil)
		select {
		case _ = <-fin.Done:

			recibido = true
			if reply[i].Success {
				nr.logger.Println("[Raft - Lider " + nr.Nodos[nr.yo] + "] Envio exitoso con " + nr.Nodos[i])

				nr.mux.Lock()
				if operacion != nil { //Si enviada operacion a registrar
					nr.nextIndex[i]++ //Aumentamos el indice de LogRegister del nodo
				}
				respuestasRecibidas++
				nr.mux.Unlock()

				//Si nos ha respondido la mayoria de nodos avisamos al lider
				if respuestasRecibidas >= ((len(nr.Nodos)-1)/2)+1 {

					terminado <- true //Avisamos al lider
					nr.logger.Println("[Raft - Lider " + nr.Nodos[nr.yo] + "] Mensaje aceptado por la mayoria de replicas")

					if operacion != nil {

						nr.mux.Lock()
						nr.commitIndex++      //Aumentamos nuestor indice de operaciones comprometidas
						nr.nextIndex[nr.yo]++ //Aumentamos nuestro indice de LogRegister
						nr.mux.Unlock()

						nr.logger.Println("[LOG] Estado actual del log:", nr.log)
					}
				}
			}
		//Pasados 150 ms reintentamos envio
		case <-time.After(150 * time.Millisecond):
			break
		}
	}

}

func (nr *NodoRaft) SometerOperacion(operacion interface{}) (int, int, bool, int, string) {

	reply := make([]RespuestaAppendEntries, len(nr.Nodos))
	terminado := make(chan bool)
	respuestasRecibidas := 0

	if nr.state == 2 { //Es lider

		if operacion != nil {
			//Guardamos la operación en nuestro LogRegister
			nr.log = append(nr.log, LogRegister{currentTerm: nr.currentTerm, operacion: operacion})
		}

		for i := 0; i < len(nr.Nodos); i++ {
			if i != nr.yo {
				go nr.ConsensoSometer(i, reply, terminado, respuestasRecibidas, operacion)
			}
		}

		_ = <-terminado //Nos bloqueamos hasta que goroutinas avisen que ha llegado mayoría de respuestas
	}

	return nr.commitIndex, nr.currentTerm, nr.state == 2, nr.IdLider, ""
}

func (nr *NodoRaft) iniciarElecciones() {

	nr.mux.Lock()
	var reply []RespuestaPeticionVoto
	reply = make([]RespuestaPeticionVoto, len(nr.Nodos))

	votos := 1             				//Nos votamos a nosotros mismos
	nr.state = 1           				//Soy candidato
	nr.enElecciones = true 				//Iniciamos eleccioner
	nr.IdLider = IntNOINICIALIZADO      //No hay lider
	nr.currentTerm++       				//Aumentamos el currentTerm al iniciar elecciones
	nr.VotedFor = nr.yo
	nr.mux.Unlock()

	args := ArgsPeticionVoto{

		Term:         nr.currentTerm,
		CandidateId:  nr.yo,
		LastLogIndex: nr.nextIndex[nr.yo] - 1,
		LastLogTerm:  (nr.log[nr.nextIndex[nr.yo]-1]).currentTerm,
	}

	fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] No he recibido latidos. Inicio elecciones. Term" + strconv.Itoa(nr.currentTerm))
	nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] No he recibido latidos. Inicio elecciones. Term" + strconv.Itoa(nr.currentTerm))

	for i, _ := range nr.Nodos { //Enviamos peticion de voto a todos los nodos
		if i != nr.yo && nr.state == 1 { //Si aun no hay lider pedimos voto
			if nr.enviarPeticionVoto(i, &args, &reply[i]) {
				nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] Solicitada votacion a " + nr.Nodos[i])
			}
		}
	}
	timer := time.After(100 * time.Millisecond) //Las elecciones durarán 100 ms

	select { //Revisamos los resultados una vez terminadas las elecciones
	case <-timer:

		for _, rep := range reply { //Contamos los votos
			if rep.VoteGranted && nr.state == 1 {
				votos++
				nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] Votacion finalizada. Votos: " + strconv.Itoa(votos))
			}
		}

		if votos >= (len(nr.Nodos)/2)+1 && nr.state == 1 { //Ganamos elecciones

			nr.recibido <- 2 //Somos lideres

			nr.mux.Lock()
			nr.state = 2
			nr.IdLider = nr.yo
			nr.enElecciones = false

			for i := 0; i < len(nr.Nodos); i++ {
				nr.nextIndex[i] = len(nr.log)
			}
			nr.mux.Unlock()

			fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] He ganado, soy LIDER.")
			nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] He ganado, soy LIDER.")

			nr.SometerOperacion(nil)

		} else { //Perdemos elecciones

			nr.mux.Lock()
			nr.state = 0 //Somos seguidores
			nr.enElecciones = false
			nr.mux.Unlock()

			fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] No he resultado ganador de las elecciones.")
			nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] No he resultado ganador de las elecciones.")
		}
	}
}

func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) error {

	nr.mux.Lock() //Realizamos la función en exclusión mutua
	defer nr.mux.Unlock()

	if nr.state == 1 { //Soy seguidor

		nr.recibido <- 1 //Recibido petición voto

		//Si nuestro currentTerm distinto al del lider, podemos votar
		if nr.currentTerm != args.Term {
			nr.VotedFor = IntNOINICIALIZADO
		}

		if !((nr.log[nr.commitIndex].currentTerm > args.LastLogTerm) || 
		((nr.log[nr.commitIndex].currentTerm == args.LastLogTerm) && (nr.lastApplied > args.LastLogIndex))) { //Si lider valido le votamos

			if (nr.VotedFor == -1 || nr.VotedFor == args.CandidateId) && (args.LastLogIndex >= nr.lastApplied) {

				nr.currentTerm = args.Term
				nr.VotedFor = args.CandidateId
				reply.VoteGranted = true
				nr.state = 0

				nr.logger.Println(strconv.Itoa(nr.VotedFor) + "[Raft - Voto" + nr.Nodos[nr.yo] + "] He votado SI. Term: " + strconv.Itoa(nr.currentTerm))
			}

		} else {

			reply.VoteGranted = false
			reply.Term = nr.currentTerm

			nr.logger.Println(strconv.Itoa(nr.VotedFor) + "[Raft - Voto" + nr.Nodos[nr.yo] + "] He votado NO. Term: " + strconv.Itoa(nr.currentTerm))
		}
	} else { //Soy lider o candidato

		reply.VoteGranted = false
		reply.Term = nr.currentTerm
		nr.logger.Println(strconv.Itoa(nr.VotedFor) + "[Raft - Voto" + nr.Nodos[nr.yo] + "] He votado NO, soy candidato/lider. Term: " + strconv.Itoa(nr.currentTerm))
	}
	return nil
}

func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) bool {

	err := rpctimeout.CallTimeout(nr.NodosDial[nodo], "NodoRaft.PedirVoto", &args, &reply, 100*time.Millisecond)
	return err == nil
}
