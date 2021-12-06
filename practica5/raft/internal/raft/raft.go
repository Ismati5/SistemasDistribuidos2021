
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
	"sync"
	"strconv"

	"raft/internal/comun/rpctimeout"
)

//  false deshabilita por completo los logs de depuracion
// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
const kEnableDebugLogs = false

// Poner a true para logear a stdout en lugar de a fichero
const kLogToStdout = false

// Cambiar esto para salida de logs en un directorio diferente
const kLogOutputDir = "/home/a796919/ssdd/practicas/practica4/raft/logs_raft/"

type AplicaOperacion struct {
	indice    int 
	operacion interface{}
}

type registro struct {
	mandato   int
	operacion interface{}
}

type NodoRaft struct {
	mux sync.Mutex //Mutex para proteger acceso a estado compartido

	Nodos  []string
	yo     int      
	estado int      //0 seguidor, 1 candidato, 2 lider
	logger       *log.Logger
	enElecciones bool
	log          []registro
	mandato      int
	Votado_A     int
	recibido     chan int
	Id_lider 	 int

	indice_comp int
	indice_reg  int

	sig_ind_log  []int
	sig_ind_comp []int

	NodosDial	[]*rpc.Client
}

func (nr *NodoRaft) InicializarCampos(nodos[]string,yo int){

	nr.Nodos = nodos
	nr.yo = yo
	nr.enElecciones = false
	nr.estado = 0
	nr.mandato = 0 //currentTerm
	nr.Votado_A = -1
	nr.indice_comp = 0 //commitIndex
	nr.indice_reg = 0 //lastApplied
	nr.Id_lider = -1

	nr.recibido = make(chan int, len(nodos))
	nr.log = make([]registro, 1) //log[]
	nr.NodosDial = make([]*rpc.Client, len(nodos))
	nr.sig_ind_log = make([]int, len(nodos)) //nextIndex[]
	nr.sig_ind_comp = make([]int, len(nodos)) //matchIndex[]

	for i := 0; i < len(nodos); i++ {
		nr.sig_ind_log[i] = 1
		nr.sig_ind_comp[i] = 0
	}
}


func (nr *NodoRaft) RedibirMensajes(){

	for {
		if (nr.estado != 2) { //No es lider

			rand.Seed(time.Now().UnixNano()) //Generador de semilla para timeout aleatorio

			select { //Si no llegan avisos de mensajes en timeout ms se inician elecciones	
			case i := <- nr.recibido: //Recibimos avisos de llegada de mensajes

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
			case <- time.After(time.Duration(150+rand.Intn(250-150))*time.Millisecond):

				//Si no hay elecciones iniciadas ya y no soy candidato ni lider
				if (!nr.enElecciones && nr.Id_lider != nr.yo && nr.estado == 0){
					go nr.iniciarElecciones()
				}
			}
		} else { //Es lider
			nr.SometerOperacion(nil) //Enviamos latido
			time.Sleep(50 * time.Millisecond) //Enviamos latido casa 50 ms
		}
	}

}

func NuevoNodo(nodos[]string,yo int,canalAplicar chan AplicaOperacion)*NodoRaft{

	var err error
	nr := &NodoRaft{}
	nr.InicializarCampos(nodos,yo)

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

	time.Sleep(1250*time.Millisecond)

	//Creamos las conexiones con todos los nodos
	for i , nodo := range nr.Nodos{
		if i != nr.yo {
			nr.NodosDial[i], err = rpc.Dial("tcp", nodo)
			fmt.Println("[" + nr.Nodos[nr.yo] + "] Conectado con " + nodo)
			nr.logger.Println("[" + nr.Nodos[nr.yo] + "] Conectado con " + nodo)
		}
	}

	fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Creado NodoRaft.")

	go nr.RedibirMensajes()
	return nr
}

type Vacio struct{}


type RepBool struct{
	Value	bool
}

func (nr *NodoRaft) Para(args Vacio, reply *Vacio) error {
	go func() {
		time.Sleep(5 * time.Millisecond)
		os.Exit(0) 
	}()
	return nil
}

func (nr *NodoRaft) ObtenerEstado(args Vacio, reply *RepBool)(int, int, bool){

	reply.Value = (nr.estado == 2)	//reply.Value = true si es lider
	return nr.yo, nr.mandato, (nr.estado == 2)
}

func (nr *NodoRaft) ObtenerEstadoRPC(args Vacio, reply *RepBool) error {

	reply.Value = (nr.estado == 2)	//reply.Value = true si es lider
	return nil
}

type ArgsAppendEntries struct {
	Mandato     int
	Id_lider    int
	Ind_log_ant int

	Mand_log_ant int
	Entrada      interface{}

	Ind_lider 	int
}

type RespuestaAppendEntries struct {
	Mandato int
	Exito   bool
}

//SOMETER OPERACIONES

func (nr *NodoRaft) AppendEntries(args *ArgsAppendEntries,reply *RespuestaAppendEntries) error {
	nr.mux.Lock()	//Realizamos la función en exclusión mutua
	defer nr.mux.Unlock()

	nr.Id_lider = args.Id_lider //Actualizamos nuestro Id_lider al actual en caso de ser nuevo mandato
	nr.mandato = args.Mandato //Actualizamos nuestro mandato al del lider
	nr.enElecciones = false //Al llegar mensaje de lider se han acabado elecciones en caso de haberlas
	nr.recibido <- 0 //Recibido mensaje

	if (args.Mandato < nr.mandato) || ((nr.log[args.Ind_log_ant]).mandato != args.Mand_log_ant) {
		reply.Exito = false
	} else {
		reply.Exito = true
		if args.Ind_lider > nr.indice_comp {
			if args.Ind_lider < args.Ind_log_ant+1 {
				nr.indice_comp = args.Ind_lider
			} else {
				nr.indice_comp = args.Ind_log_ant+1
			}
		}
	}

	if args.Entrada != nil { //El mensaje no es un latido
		//Lo introducimos en el registro
		nr.log = append(nr.log, registro{mandato: nr.mandato, operacion: args.Entrada})
		
		fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Me ha llegado una operacion de tipo",args.Entrada)
		nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + "] Me ha llegado una operacion de tipo",args.Entrada)
		nr.logger.Println("[LOG] Estado actual del log:",nr.log)
	}else{
		fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Me ha llegado latido.")
		nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + "] Me ha llegado latido.")
	}

	reply.Mandato = nr.mandato //Le enviamos nuestro mandato al lider
	return nil
}

func (nr *NodoRaft) SometerOperacion(operacion interface{}) (int, int, bool) {

	reply := make([]RespuestaAppendEntries, len(nr.Nodos))
	terminado := make(chan bool) 
	respuestas_recibidas := 0
	
	if (nr.estado == 2){ //Es lider

		if(operacion != nil){
			//Guardamos la operación en nuestro registro
			nr.log = append(nr.log,registro{mandato: nr.mandato, operacion: operacion})
		}

		for i := 0; i < len(nr.Nodos); i++ {
			if (i != nr.yo){
				go func (i int){
					recibido := false

					fmt.Println("[Raft - Lider " + nr.Nodos[nr.yo] + "] Enviado mensaje a : " + nr.Nodos[i])
					nr.logger.Println("[Raft - Lider " + nr.Nodos[nr.yo] + "] Enviado mensaje a : " + nr.Nodos[i])

					//Inicializamos los argumentos
					args := ArgsAppendEntries{
						Mandato: nr.mandato,   
						Id_lider: nr.yo,   
						Ind_log_ant: nr.sig_ind_log[i]-1,
						Mand_log_ant: (nr.log[nr.sig_ind_log[i]-1]).mandato,
						Entrada: operacion,
						Ind_lider: nr.indice_comp,
					}

					//Inicializamos la respuesta
					reply[i] = RespuestaAppendEntries{
						Mandato : 0,
						Exito : false,
					}

					for !recibido { //Bucle infinito hasta que nos responda el nodo i

						fin := nr.NodosDial[i].Go("NodoRaft.AppendEntries",&args, &reply[i], nil)
						select{
						case _ = <- fin.Done:

							recibido = true
							if(reply[i].Exito){
								nr.logger.Println("[Raft - Lider " + nr.Nodos[nr.yo] +"] Envio exitoso con " + nr.Nodos[i])

								nr.mux.Lock()
								if(operacion != nil){ //Si enviada operacion a registrar
									nr.sig_ind_log[i]++ //Aumentamos el indice de registro del nodo
								}
								respuestas_recibidas++
								nr.mux.Unlock()

								//Si nos ha respondido la mayoria de nodos avisamos al lider
								if(respuestas_recibidas >= ((len(nr.Nodos)-1)/2)+1){

									terminado <- true	//Avisamos al lider
									nr.logger.Println("[Raft - Lider " + nr.Nodos[nr.yo] + "] Mensaje aceptado por la mayoria de replicas")

									if(operacion != nil){

										nr.mux.Lock()
										nr.indice_comp++ //Aumentamos nuestor indice de operaciones comprometidas
										nr.sig_ind_log[nr.yo]++ //Aumentamos nuestro indice de registro
										nr.mux.Unlock()

										nr.logger.Println("[LOG] Estado actual del log:",nr.log)
									}
								}
							}
						//Pasados 150 ms reintentamos envio
						case <- time.After(150 * time.Millisecond):
							break
						}
					}
				}(i)
			}
		}

		_ = <- terminado //Nos bloqueamos hasta que goroutinas avisen que ha llegado mayoría de respuestas
	}

	return nr.indice_comp, nr.mandato, (nr.estado == 2)
}

//ELECCIONES

type ArgsPeticionVoto struct {
	Mandato_candidato int //term
	Id_candidato      int //candidateId
	Ind_ult_log_cand  int //lastLogIndex
	Mand_ult_log_cand int //lastLogTerm
}

type RespuestaPeticionVoto struct {
	Mandato int
	Votado  bool
}

func (nr *NodoRaft) iniciarElecciones() {

	nr.mux.Lock()
	var reply []RespuestaPeticionVoto
	reply = make([]RespuestaPeticionVoto, len(nr.Nodos))
	
	votos := 1 //Nos votamos a nosotros mismos
	nr.estado = 1 //Soy candidato
	nr.enElecciones = true //Iniciamos eleccioner
	nr.Id_lider = -1 //No hay lider
	nr.mandato++ //Aumentamos el mandato al iniciar elecciones
	nr.Votado_A = nr.yo 
	nr.mux.Unlock()
	
	args := ArgsPeticionVoto{

		Mandato_candidato: nr.mandato,
		Id_candidato:      nr.yo,
		Ind_ult_log_cand:  nr.sig_ind_log[nr.yo]-1,
		Mand_ult_log_cand: (nr.log[nr.sig_ind_log[nr.yo]-1]).mandato,
	}

	fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] No he recibido latidos. Inicio elecciones. Mandato" + strconv.Itoa(nr.mandato))
	nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] No he recibido latidos. Inicio elecciones. Mandato" + strconv.Itoa(nr.mandato))


	for i, _ := range nr.Nodos { //Enviamos peticion de voto a todos los nodos
		if i != nr.yo  && nr.estado == 1{ //Si aun no hay lider pedimos voto
			if (nr.enviarPeticionVoto(i, &args, &reply[i])){
				nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] Solicitada votacion a " + nr.Nodos[i])
			}
		}
	}
	timer := time.After(100 * time.Millisecond) //Las elecciones durarán 100 ms

	select { //Revisamos los resultados una vez terminadas las elecciones
	case <- timer:

		for _,rep := range reply { //Contamos los votos
			if rep.Votado && nr.estado == 1{
				votos++
				nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] Votacion finalizada. Votos: " + strconv.Itoa(votos))
			}
		}

		if (votos >= (len(nr.Nodos)/2)+1 && nr.estado == 1)  { //Ganamos elecciones

			nr.recibido <- 2 //Somos lideres

			nr.mux.Lock()
			nr.estado = 2
			nr.Id_lider = nr.yo
			nr.enElecciones = false

			for i := 0; i < len(nr.Nodos); i++ {
				nr.sig_ind_log[i] = len(nr.log)
			}
			nr.mux.Unlock()

			fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] He ganado, soy LIDER.")
			nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] He ganado, soy LIDER.")	

			nr.SometerOperacion(nil)

		} else { //Perdemos elecciones

			nr.mux.Lock()
			nr.estado = 0 //Somos seguidores
			nr.enElecciones = false
			nr.mux.Unlock()

			fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] No he resultado ganador de las elecciones.")
			nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + "] No he resultado ganador de las elecciones.")
		}
	}
}

func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto,reply *RespuestaPeticionVoto) error {
	
	nr.mux.Lock() //Realizamos la función en exclusión mutua
	defer nr.mux.Unlock()

	if (nr.estado == 1){ //Soy seguidor

		nr.recibido <- 1 //Recibido petición voto

		//Si nuestro mandato distinto al del lider, podemos votar
		if(nr.mandato != args.Mandato_candidato){
			nr.Votado_A = -1
		}

		if args.Mandato_candidato >= nr.mandato { //Si lider valido le votamos

			if (nr.Votado_A == -1 || nr.Votado_A == args.Id_candidato) && (args.Ind_ult_log_cand >= nr.indice_reg) {
				
				nr.mandato = args.Mandato_candidato
				nr.Votado_A = args.Id_candidato
				reply.Votado = true
				nr.estado = 0

				nr.logger.Println(strconv.Itoa(nr.Votado_A) + "[Raft - Voto" + nr.Nodos[nr.yo] + "] He votado SI. Mandato: " + strconv.Itoa(nr.mandato))
			}

		} else { 

			reply.Votado = false
			reply.Mandato = nr.mandato

			nr.logger.Println(strconv.Itoa(nr.Votado_A) + "[Raft - Voto" + nr.Nodos[nr.yo] + "] He votado NO. Mandato: " + strconv.Itoa(nr.mandato))
		}
	} else { //Soy lider o candidato

		reply.Votado = false
		reply.Mandato = nr.mandato
		nr.logger.Println(strconv.Itoa(nr.Votado_A) + "[Raft - Voto" + nr.Nodos[nr.yo] + "] He votado NO, soy candidato/lider. Mandato: " + strconv.Itoa(nr.mandato))
	}
	return nil
}

func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,reply *RespuestaPeticionVoto) bool {

	err := rpctimeout.CallTimeout(nr.NodosDial[nodo], "NodoRaft.PedirVoto", &args, &reply, 100*time.Millisecond)
	return err == nil
}
