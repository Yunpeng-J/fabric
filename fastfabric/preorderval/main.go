package main

import (
	"flag"
	"fmt"
	"github.com/hyperledger/fabric/fastfabric/preorderval/validator"
)

func main() {
	fmt.Println("Starting validation server")
	var address = flag.String("address", "localhost", "accepts all txs if set")
	var mock = flag.Bool("x", false, "accepts all txs if set")
	flag.Parse()
	validator.StartServer(*address, *mock)
}
