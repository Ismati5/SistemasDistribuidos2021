package main

import (
	"fmt"
	"os"
	"net/rpc"
	"raft/internal/comun/rpctimeout"
	"time"
)


type Args struct {
	A, B int
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func main() {

	args := os.Args
	var reply int
	argumentos := Args{
		A : 5,
		B : 7,
	}

	client, err := rpc.Dial("tcp", args[1])
	checkError(err)

	fmt.Println("[Cliente] Conectado con " + args[1])

	err = rpctimeout.CallTimeout(client,
		"Arith.Mul", &argumentos, &reply, 750 * time.Millisecond)
	checkError(err)

	fmt.Println("Arith:",argumentos.A,"*",argumentos.B,"=",reply)
}
