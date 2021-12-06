package main

import (
	"fmt"
	"os"
	"net"
	//"net/http"
	"net/rpc"
	"raft/internal/raft"
	"raft/internal/comun/rpctimeout"
	"time"
	"strconv"
)

type Args struct {
	A, B int
}

type Arith int

func (t *Arith) Mul(args *Args, reply *int) error {

	var replay raft.RepBool
	_,_,esLider := nr.ObtenerEstado(raft.Vacio{}, &replay)

	if !esLider {

		if nr.Id_lider != -1 {
			fmt.Println("[" + os.Args[ID+2] + "] Redirigiendo peticion a lider.")
			err := rpctimeout.CallTimeout(nr.NodosDial[nr.Id_lider],
				"Arith.Mul", &args, &reply, 500 * time.Millisecond)
			checkError(err)
		}

	}else{
		nr.SometerOperacion("Arith.Mul")
		*reply = args.A * args.B

	}
	return nil
}	

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

var nr *raft.NodoRaft
var ID int

func main() {

	ID, _ := strconv.Atoi(os.Args[1])

	arith := new(Arith)
	rpc.Register(arith)

	var canalAplicar = make (chan raft.AplicaOperacion, 5)

	l, err := net.Listen("tcp", os.Args[2:][ID])
	checkError(err)
	
	fmt.Println("[" + os.Args[2:][ID] + "] nodo ha hecho listen.")

	nr = raft.NuevoNodo(os.Args[2:], ID, canalAplicar)
	rpc.Register(nr)
	fmt.Println("[" + os.Args[ID+2] + "] NodoRaft del main creado.")

	rpc.Accept(l)

}
