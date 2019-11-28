package dependency

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"strings"
	"sync"
)

var compositeKeySep = string([]byte{0x00})

type analyzer struct {
	keyDPs             chan map[string]chan *skipList
	currentBlock       chan uint64
	futureKnowledge    chan map[uint64]bool
	outputPerBlock     chan map[uint64]chan *Transaction
	blockedTxsPerBlock chan map[uint64]map[string]*Transaction
	committedTxs       chan *Transaction
	done               chan bool
	committingTxs      chan map[string]*Transaction
	once               sync.Once
	input              chan uint64
}

func (a *analyzer) Stop() {
	a.once.Do(func() {
		close(a.done)
	})
}

func (a *analyzer) NotifyAboutCommit(tx *Transaction) {
	a.committedTxs <- tx
}

type Analyzer interface {
	Analyze(block *cached.Block) (<-chan *Transaction, error)
	NotifyAboutCommit(*Transaction)
	Stop()
}

func NewAnalyzer() Analyzer {
	a := &analyzer{
		keyDPs:             make(chan map[string]chan *skipList, 1),
		currentBlock:       make(chan uint64, 1),
		futureKnowledge:    make(chan map[uint64]bool, 1),
		outputPerBlock:     make(chan map[uint64]chan *Transaction, 1),
		committedTxs:       make(chan *Transaction, 1000),
		done:               make(chan bool),
		committingTxs:      make(chan map[string]*Transaction, 1),
		blockedTxsPerBlock: make(chan map[uint64]map[string]*Transaction, 1),
		input:              make(chan uint64, 5),
	}
	a.keyDPs <- make(map[string]chan *skipList)
	a.currentBlock <- 0
	a.futureKnowledge <- make(map[uint64]bool)
	a.outputPerBlock <- make(map[uint64]chan *Transaction)
	a.committingTxs <- make(map[string]*Transaction)
	a.blockedTxsPerBlock <- make(map[uint64]map[string]*Transaction)
	go func() {
		for {
			select {
			case _, more := <-a.done:
				if !more {
					return
				}
			case committedTx := <-a.committedTxs:
				a.removeDependencies(committedTx)

			case blockNum := <-a.input:
				blocks := <-a.blockedTxsPerBlock
				for _, t := range blocks[blockNum] {
					a.addDependencies(t)
				}
				a.blockedTxsPerBlock <- blocks
				if blockNum == 0 || a.updateWatermark(blockNum) {
					a.tryReleaseIdpTxs()
				}
			}
		}
	}()
	return a
}

func (a *analyzer) Analyze(b *cached.Block) (<-chan *Transaction, error) {
	blockNum := b.Header.Number
	_ = b.UnmarshalAll()
	envs, err := b.UnmarshalAllEnvelopes()
	if err != nil {
		return nil, err
	}
	txs := map[string]*Transaction{}
	for txNum, env := range envs {
		pl, err := env.UnmarshalPayload()
		if err != nil {
			return nil, err
		}

		t, err := NewTransaction(blockNum, uint64(txNum), pl)
		if err != nil {
			return nil, err
		}
		txs[t.TxID] = t
	}

	output := make(chan *Transaction, len(txs))
	opb := <-a.outputPerBlock
	opb[blockNum] = output
	a.outputPerBlock <- opb
	btx := <-a.blockedTxsPerBlock
	btx[blockNum] = txs
	a.blockedTxsPerBlock <- btx

	a.input <- blockNum
	return output, nil
}

func (a *analyzer) addDependencies(tx *Transaction) {
	if tx.RwSet == nil {
		return
	}
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			if strings.HasPrefix(w.Key, "oracle_") {
				continue
			}
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			listChan := a.getDPListForKey(compKey)
			list := <-listChan
			list.AddWrite(tx)
			listChan <- list
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			listChan := a.getDPListForKey(compKey)
			list := <-listChan
			list.AddRead(tx)
			listChan <- list
		}
	}
}

func (a *analyzer) release(tx *Transaction) {
	ctx := <-a.committingTxs
	if _, ok := ctx[tx.TxID]; !ok {
		ctx[tx.TxID] = tx
		a.committingTxs <- ctx

		opb := <-a.outputPerBlock
		opb[tx.Version.BlockNum] <- tx
		btx := <-a.blockedTxsPerBlock
		delete(btx[tx.Version.BlockNum], tx.TxID)
		//fmt.Printf("Tx left in block [%d]: %d\n", tx.Version.BlockNum, btx[tx.Version.BlockNum] )
		if len(btx[tx.Version.BlockNum]) == 0 {
			delete(btx, tx.Version.BlockNum)
			close(opb[tx.Version.BlockNum])
		}
		a.blockedTxsPerBlock <- btx
		a.outputPerBlock <- opb
	} else {
		a.committingTxs <- ctx
	}
}

func (a *analyzer) getDPListForKey(compKey string) chan *skipList {
	kdp := <-a.keyDPs
	listChan, ok := kdp[compKey]
	if !ok {
		listChan = make(chan *skipList, 1)
		list := NewSkipList()
		listChan <- list
		kdp[compKey] = listChan
	}
	a.keyDPs <- kdp
	return listChan
}

func (a *analyzer) removeDependencies(tx *Transaction) {
	if tx.RwSet == nil {
		return
	}
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			listChan := a.getDPListForKey(compKey)
			list := <-listChan
			list.Delete(tx.TxID)
			first := list.First()
			listChan <- list
			if first != nil {
				for _, el := range first.elements {
					a.tryRelease(el.Transaction)
				}
			}
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			listChan := a.getDPListForKey(compKey)
			list := <-listChan
			list.Delete(tx.TxID)
			first := list.First()
			listChan <- list
			if first != nil {
				for _, el := range first.elements {
					a.tryRelease(el.Transaction)
				}
			}
		}
	}
}

func (a *analyzer) updateWatermark(analyzedBlockNum uint64) (changed bool) {
	cbl := <-a.currentBlock
	originlCbl := cbl
	defer func() {
		a.currentBlock <- cbl
	}()

	fk := <-a.futureKnowledge
	defer func() { a.futureKnowledge <- fk }()

	if analyzedBlockNum == cbl+1 {
		cbl += 1
	} else {
		fk[analyzedBlockNum] = true
	}

	for i := cbl + 1; fk[i]; i++ {
		cbl += 1
		delete(fk, i)
	}

	return cbl != originlCbl
}

func (a *analyzer) tryReleaseIdpTxs() {
	cbl := <-a.currentBlock
	blocked := <-a.blockedTxsPerBlock
	for blockNum, txs := range blocked {
		if blockNum > cbl {
			continue
		}

		for txID, tx := range txs {
			deps := <-tx.dependencies
			depCount := len(deps)
			tx.dependencies <- deps
			if depCount != 0 {
				continue
			}

			ctx := <-a.committingTxs
			if _, ok := ctx[tx.TxID]; !ok {
				ctx[tx.TxID] = tx
				a.committingTxs <- ctx

				opb := <-a.outputPerBlock
				opb[tx.Version.BlockNum] <- tx

				delete(blocked[blockNum], txID)
				if len(blocked[blockNum]) == 0 {
					delete(blocked, blockNum)
					close(opb[tx.Version.BlockNum])
				}

				a.outputPerBlock <- opb
			} else {
				a.committingTxs <- ctx
			}
		}
	}

	a.blockedTxsPerBlock <- blocked
	a.currentBlock <- cbl
}

func (a *analyzer) tryRelease(transaction *Transaction) {
	deps := <-transaction.dependencies
	if len(deps) == 0 {
		a.release(transaction)
	}
	transaction.dependencies <- deps
}

type Transaction struct {
	Version      *version.Height
	Payload      *cached.Payload
	TxID         string
	dependencies chan map[string]*Transaction

	RwSet          *cached.TxRwSet
	ValidationCode peer.TxValidationCode
}

func NewTransaction(blockNum uint64, txNum uint64, payload *cached.Payload) (*Transaction, error) {
	chdr, err := payload.Header.UnmarshalChannelHeader()
	if err != nil {
		return nil, err
	}

	tx, err := payload.UnmarshalTransaction()
	if err != nil {
		return nil, err
	}
	acPl, err := tx.Actions[0].UnmarshalChaincodeActionPayload()
	if err != nil {
		return nil, err
	}

	txType := common.HeaderType(chdr.Type)
	deps := make(chan map[string]*Transaction, 1)
	deps <- map[string]*Transaction{}
	if txType != common.HeaderType_ENDORSER_TRANSACTION {
		return &Transaction{Version: &version.Height{BlockNum: blockNum, TxNum: txNum},
			TxID:         chdr.TxId,
			Payload:      payload,
			RwSet:        nil,
			dependencies: deps,
		}, nil
	}

	respPl, err := acPl.Action.UnmarshalProposalResponsePayload()
	if err != nil {
		return nil, err
	}
	ccAc, err := respPl.UnmarshalChaincodeAction()
	if err != nil {
		return nil, err
	}
	rwset, err := ccAc.UnmarshalRwSet()
	if err != nil {
		return nil, err
	}
	return &Transaction{Version: &version.Height{BlockNum: blockNum, TxNum: txNum},
		TxID:         chdr.TxId,
		Payload:      payload,
		RwSet:        rwset,
		dependencies: deps}, nil

}

func (tx *Transaction) addDependency(other *Transaction) {
	deps := <-tx.dependencies
	defer func() { tx.dependencies <- deps }()

	if _, ok := deps[other.TxID]; other == tx || ok {
		return
	}

	deps[other.TxID] = other
}

func (tx *Transaction) removeDependency(other *Transaction) {
	deps := <-tx.dependencies
	delete(deps, other.TxID)
	tx.dependencies <- deps
}

// ContainsPvtWrites returns true if this transaction is not limited to affecting the public data only
func (t *Transaction) ContainsPvtWrites() bool {
	for _, ns := range t.RwSet.NsRwSets {
		for _, coll := range ns.CollHashedRwSets {
			if coll.PvtRwSetHash != nil {
				return true
			}
		}
	}
	return false
}

// RetrieveHash returns the hash of the private write-set present
// in the public data for a given namespace-collection
func (t *Transaction) RetrieveHash(ns string, coll string) []byte {
	if t.RwSet == nil {
		return nil
	}
	for _, nsData := range t.RwSet.NsRwSets {
		if nsData.NameSpace != ns {
			continue
		}

		for _, collData := range nsData.CollHashedRwSets {
			if collData.CollectionName == coll {
				return collData.PvtRwSetHash
			}
		}
	}
	return nil
}

func constructCompositeKey(ns string, key string) string {
	var buffer strings.Builder
	buffer.WriteString(ns)
	buffer.WriteString(compositeKeySep)
	buffer.WriteString(key)
	return buffer.String()
}
