package main

import (
	"fmt"
	"github.com/hyperledger/fabric/fastfabric/preorderval/validator"
	"os"
)

func main() {
	fmt.Println("Starting validation server")
	validator.StartServer(os.Args[1])
}
