package parallel

import (
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/fastfabric/config"
	"github.com/hyperledger/fabric/fastfabric/dependency"
)

var ReadyToCommit = make(chan chan *cached.Block, config.BlockPipelineWidth)
var ReadyForValidation = make(chan *Pipeline, config.BlockPipelineWidth)

//This is a randomly chosen value >> threads working on committing
var AnalyzedTxs = make(chan *dependency.Transaction, 200)

type Pipeline struct {
	Channel chan *cached.Block
	Block   *cached.Block
}
