
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

func NuevoNodo(nodos[]string,yo int,canalAplicar chan AplicaOperacion)*NodoRaft{

	var err error
	nr := &NodoRaft{}
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

	fmt.Println("[Raft " + nr.Nodos[nr.yo] + "] Creado NodoRaft.")

	time.Sleep(1250*time.Millisecond)

	for i , nodo := range nr.Nodos{
		if i != nr.yo {
			nr.logger.Println("[" + nr.Nodos[nr.yo] + 
				"] Intento conectar con " + nodo)
			nr.NodosDial[i], err = rpc.Dial("tcp", nodo)
			fmt.Println("[" + nr.Nodos[nr.yo] + 
				"] Conectado con " + nodo)
			nr.logger.Println("[" + nr.Nodos[nr.yo] + 
				"] Conectado con " + nodo)
		}
	}

	go func() {
		for {
			if (nr.estado != 2) { //No es lider

				rand.Seed(time.Now().UnixNano())

				select {
				case i := <- nr.recibido:
					if(i == 0){
						fmt.Println("[Raft " + nr.Nodos[nr.yo] + 
							"] Recibido latido.")
						nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + 
							"] Recibido latido.")
					} else if (i == 1){
						fmt.Println("[Raft " + nr.Nodos[nr.yo] + 
							"] Recibido peticion voto.")
						nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + 
							"] Recibido peticion voto.")
					} else if (i == 2){
						fmt.Println("[Raft " + nr.Nodos[nr.yo] + 
							"] Me convierto en lider.")
						nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + 
							"] Me convierto en lider.")
					}
				case <- time.After(time.Duration(150+rand.Intn(250-150))*time.Millisecond):
					if (!nr.enElecciones && nr.Id_lider != nr.yo && nr.estado == 0){
						go nr.iniciarElecciones()
					}
				}
			} else { //Es lider
				nr.SometerOperacion(nil) //Enviamos latido
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	return nr
}

type Vacio struct{}

type RepBool struct{
	Value	bool
}

func (nr *NodoRaft) Para(args Vacio, reply *Vacio) error {
	go func() {time.Sleep(5 * time.Millisecond); os.Exit(0) } ()
	return nil
}

func (nr *NodoRaft) ObtenerEstado(args Vacio, reply *RepBool)(int, int, bool){

	reply.Value = (nr.estado == 2)

	return nr.yo, nr.mandato, (nr.estado == 2)
}

func (nr *NodoRaft) ObtenerEstadoRPC(args Vacio, reply *RepBool) error {

	reply.Value = (nr.estado == 2)

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

func (nr *NodoRaft) AppendEntries(args *ArgsAppendEntries,
	reply *RespuestaAppendEntries) error {
	nr.mux.Lock()
	defer nr.mux.Unlock()

	nr.Id_lider = args.Id_lider
	nr.enElecciones = false
	nr.mandato = args.Mandato
	nr.recibido <- 0

	if (args.Mandato < nr.mandato) || 
	((nr.log[args.Ind_log_ant]).mandato != args.Mand_log_ant) {
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

	if args.Entrada != nil {
		nr.log = append(nr.log, 
			registro{mandato: nr.mandato, operacion: args.Entrada})
		fmt.Println("[Raft " + nr.Nodos[nr.yo] + 
			"] Me ha llegado una operacion de tipo",args.Entrada)
		nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + 
			"] Me ha llegado una operacion de tipo",args.Entrada)
		nr.logger.Println("[LOG] Estado actual del log:",nr.log)
	}else{
		fmt.Println("[Raft " + nr.Nodos[nr.yo] + 
			"] Me ha llegado latido.")
		nr.logger.Println("[Raft " + nr.Nodos[nr.yo] + 
			"] Me ha llegado latido.")
	}

	reply.Mandato = nr.mandato
	return nil
}

func (nr *NodoRaft) SometerOperacion(operacion interface{}) (int, int, bool) {

	reply := make([]RespuestaAppendEntries, len(nr.Nodos))
	terminado := make(chan bool) 

	respuestas_recibidas := 0
	
	if (nr.estado == 2){	//Es lider

		if(operacion != nil){
			nr.log = append(nr.log, 
				registro{mandato: nr.mandato, operacion: operacion})
		}

		for i := 0; i < len(nr.Nodos); i++ {
			if (i != nr.yo){
				go func(i int) {
					recibido := false

					fmt.Println("[Raft - Lider " + nr.Nodos[nr.yo] + 
						"] Enviado mensaje a : " + nr.Nodos[i])
					nr.logger.Println("[Raft - Lider " + nr.Nodos[nr.yo] + 
						"] Enviado mensaje a : " + nr.Nodos[i])

					args := ArgsAppendEntries{
						Mandato: nr.mandato,   
						Id_lider: nr.yo,   
						Ind_log_ant: nr.sig_ind_log[i]-1,
						Mand_log_ant: (nr.log[nr.sig_ind_log[i]-1]).mandato,
						Entrada: operacion,
						Ind_lider: nr.indice_comp,
					}

					reply[i] = RespuestaAppendEntries{
						Mandato : 0,
						Exito : false,
					}

					for !recibido {

						fin := nr.NodosDial[i].Go("NodoRaft.AppendEntries",
							&args, &reply[i], nil)
						select{
						case _ = <- fin.Done:
							if(reply[i].Exito){
								nr.logger.Println("[Raft - Lider " + 
									nr.Nodos[nr.yo] +
									"] Envio exitoso con " + nr.Nodos[i])
								nr.mux.Lock()

								if(operacion != nil){
									nr.sig_ind_log[i]++
								}

								respuestas_recibidas++
								
								nr.mux.Unlock()

								if(respuestas_recibidas>=((len(nr.Nodos)-1)/2)+1){
									terminado <- true	//Avisamos al lider
									nr.logger.Println("[Raft - Lider " + 
										nr.Nodos[nr.yo] + 
										"] Mensaje aceptado por la mayoria de replicas")
									if(operacion != nil){
										nr.mux.Lock()

										nr.indice_comp++
										nr.sig_ind_log[nr.yo]++

										nr.mux.Unlock()
										nr.logger.Println("[LOG] Estado actual del log:",
											nr.log)
									}
									
								}
							}
							recibido = true
						case <- time.After(150 * time.Millisecond):
							break
						}
					}
				}(i)
			}
		}

	_ = <- terminado

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
	votos := 1

	reply = make([]RespuestaPeticionVoto, len(nr.Nodos))

	nr.estado = 1
	nr.enElecciones = true
	nr.Id_lider = -1

	nr.mandato++
	nr.Votado_A = nr.yo

	nr.mux.Unlock()
	

	args := ArgsPeticionVoto{

		Mandato_candidato: nr.mandato,
		Id_candidato:      nr.yo,
		Ind_ult_log_cand:  nr.sig_ind_log[nr.yo]-1,
		Mand_ult_log_cand: (nr.log[nr.sig_ind_log[nr.yo]-1]).mandato,
	}

	fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + 
		"] No he recibido latidos. Inicio elecciones. Mandato" + 
		strconv.Itoa(nr.mandato))
	nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + 
		"] No he recibido latidos. Inicio elecciones. Mandato" + 
		strconv.Itoa(nr.mandato))
	votos = 1
	timer := time.After(100 * time.Millisecond)

	for i, _ := range nr.Nodos {
		if i != nr.yo {
			if (nr.estado == 1){
				if (nr.enviarPeticionVoto(i, &args, &reply[i])){
					nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + 
						"] Solicitada votacion a " + nr.Nodos[i])
				}
			}
		}
	}

	select {
	case <- timer:
		for _,rep := range reply {
			if rep.Votado && nr.estado == 1{
				votos++
				nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + 
					"] Votacion finalizada. Votos: " + strconv.Itoa(votos))
			}
		}

		if (votos >= (len(nr.Nodos)/2)+1 && nr.estado == 1)  { //Gana elecciones
			nr.recibido <- 2
			nr.mux.Lock()
			nr.estado = 2
			nr.Id_lider = nr.yo
			fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + 
				"] He ganado, soy LIDER.")
			nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + 
				"] He ganado, soy LIDER.")	
			nr.enElecciones = false
			for i := 0; i < len(nr.Nodos); i++ {
				nr.sig_ind_log[i] = len(nr.log)
			}
			nr.mux.Unlock()
			nr.SometerOperacion(nil)
		} else {
			nr.mux.Lock()
			nr.estado = 0
			nr.enElecciones = false
			nr.mux.Unlock()
			fmt.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + 
				"] No he resultado ganador de las elecciones.")
			nr.logger.Println("[Raft - Votacion " + nr.Nodos[nr.yo] + 
				"] No he resultado ganador de las elecciones.")
		}
	}
}

func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	nr.mux.Lock()
	defer nr.mux.Unlock()

	if (nr.estado == 0 || nr.estado == 2){

		nr.recibido <- 1

		if(nr.mandato != args.Mandato_candidato){
			nr.Votado_A = -1
		}

		if args.Mandato_candidato >= nr.mandato {
			if (nr.Votado_A == -1 || nr.Votado_A == args.Id_candidato) &&
				(args.Ind_ult_log_cand >= nr.indice_reg) {

				nr.Votado_A = args.Id_candidato
				reply.Votado = true
				nr.mandato = args.Mandato_candidato
				nr.estado = 0
				nr.logger.Println(strconv.Itoa(nr.Votado_A) + "[Raft - Voto" + 
					nr.Nodos[nr.yo] + 
					"] He votado SI. Mandato: " + strconv.Itoa(nr.mandato))
			}

		} else {
			reply.Votado = false
			reply.Mandato = nr.mandato
			nr.logger.Println(strconv.Itoa(nr.Votado_A) + "[Raft - Voto" + 
				nr.Nodos[nr.yo] + 
				"] He votado NO. Mandato: " + strconv.Itoa(nr.mandato))
		}
	} else {
		reply.Votado = false
		reply.Mandato = nr.mandato
		nr.logger.Println(strconv.Itoa(nr.Votado_A) + "[Raft - Voto" + 
			nr.Nodos[nr.yo] + 
			"] He votado NO, soy candidato/lider. Mandato: " + 
			strconv.Itoa(nr.mandato))
	}

	return nil
}

func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	err := rpctimeout.CallTimeout(nr.NodosDial[nodo], 
		"NodoRaft.PedirVoto", &args, &reply, 100*time.Millisecond)
	return err == nil
}
