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
	keyDPs             map[string]*skipList
	highWatermark      uint64 //all txs are known up to here
	lowWatermark       uint64 //all txs are unblocked up to here
	futureKnowledge    map[uint64]bool
	outputPerBlock     map[uint64]chan *Transaction
	outputLock         sync.RWMutex
	blockedTxsPerBlock chan map[uint64]map[uint64]*Transaction
	committedTxs       chan *Transaction

	done          chan bool
	committingTxs map[string]*Transaction
	once          sync.Once
	input         chan uint64
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
		keyDPs:             make(map[string]*skipList),
		highWatermark:      0,
		lowWatermark:       0,
		futureKnowledge:    make(map[uint64]bool),
		outputPerBlock:     make(map[uint64]chan *Transaction),
		committedTxs:       make(chan *Transaction, 1000),
		done:               make(chan bool),
		committingTxs:      make(map[string]*Transaction),
		blockedTxsPerBlock: make(chan map[uint64]map[uint64]*Transaction, 1),
		input:              make(chan uint64, 5),
	}
	a.blockedTxsPerBlock <- make(map[uint64]map[uint64]*Transaction)
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
				for len(a.committedTxs) > 0 {
					a.removeDependencies(<-a.committedTxs)
				}
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
	envs, err := b.UnmarshalAllEnvelopes()
	if err != nil {
		return nil, err
	}
	txs := map[uint64]*Transaction{}
	for txNum, env := range envs {
		pl, err := env.UnmarshalPayload()
		if err != nil {
			return nil, err
		}

		t, err := NewTransaction(blockNum, uint64(txNum), pl)
		if err != nil {
			return nil, err
		}
		txs[t.Version.TxNum] = t
	}

	output := make(chan *Transaction, len(txs))
	a.outputLock.Lock()
	a.outputPerBlock[blockNum] = output
	a.outputLock.Unlock()
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
			list := a.getDPListForKey(compKey)
			list.AddWrite(tx)
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			list := a.getDPListForKey(compKey)
			list.AddRead(tx)
		}
	}
}

func (a *analyzer) getDPListForKey(compKey string) *skipList {
	list, ok := a.keyDPs[compKey]
	if !ok {
		list = NewSkipList()
		a.keyDPs[compKey] = list

	}
	return list
}

func (a *analyzer) removeDependencies(tx *Transaction) {
	if tx.RwSet == nil {
		return
	}
	txsToRelease := map[string]*Transaction{}
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			list := a.getDPListForKey(compKey)
			list.Delete(tx.TxID)
			first := list.First()
			if first != nil {
				for _, el := range first.elements {
					txsToRelease[el.Transaction.TxID] = el.Transaction
				}
			}
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			list := a.getDPListForKey(compKey)
			list.Delete(tx.TxID)
			first := list.First()
			if first != nil {
				for _, el := range first.elements {
					txsToRelease[el.Transaction.TxID] = el.Transaction
				}
			}
		}

		for _, tx := range txsToRelease {
			a.tryRelease(tx)
		}
	}
}

func (a *analyzer) updateWatermark(analyzedBlockNum uint64) (changed bool) {
	originlCbl := a.highWatermark

	if analyzedBlockNum == a.highWatermark+1 {
		a.highWatermark += 1
	} else {
		a.futureKnowledge[analyzedBlockNum] = true
	}

	for i := a.highWatermark + 1; a.futureKnowledge[i]; i++ {
		a.highWatermark += 1
		delete(a.futureKnowledge, i)
	}

	return a.highWatermark != originlCbl
}

func (a *analyzer) tryReleaseIdpTxs() {
	blocked := <-a.blockedTxsPerBlock
	for i := a.lowWatermark; i <= a.highWatermark; i++ {
		if txs, ok := blocked[i]; ok {
			for _, tx := range txs {
				a.blockedTxsPerBlock <- blocked
				a.tryRelease(tx)
				blocked = <-a.blockedTxsPerBlock
			}
		}
	}

	a.blockedTxsPerBlock <- blocked
}

func (a *analyzer) tryRelease(tx *Transaction) {
	if len(tx.dependencies) != 0 {
		return
	}

	if _, ok := a.committingTxs[tx.TxID]; !ok {
		a.committingTxs[tx.TxID] = tx

		a.outputLock.RLock()
		output := a.outputPerBlock[tx.Version.BlockNum]
		a.outputLock.RUnlock()
		output <- tx
		btx := <-a.blockedTxsPerBlock
		delete(btx[tx.Version.BlockNum], tx.Version.TxNum)
		if len(btx[tx.Version.BlockNum]) == 0 {
			delete(btx, tx.Version.BlockNum)
			if a.lowWatermark < tx.Version.BlockNum {
				a.lowWatermark = tx.Version.BlockNum
			}
			close(output)
		}
		a.blockedTxsPerBlock <- btx
	}
}

type Transaction struct {
	Version      *version.Height
	Payload      *cached.Payload
	TxID         string
	dependencies map[string]*Transaction

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
	if txType != common.HeaderType_ENDORSER_TRANSACTION {
		return &Transaction{Version: &version.Height{BlockNum: blockNum, TxNum: txNum},
			TxID:         chdr.TxId,
			Payload:      payload,
			RwSet:        nil,
			dependencies: map[string]*Transaction{},
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
		dependencies: map[string]*Transaction{}}, nil

}

func (tx *Transaction) addDependency(other *Transaction) {
	if other == tx {
		return
	}

	tx.dependencies[other.TxID] = other
}

func (tx *Transaction) removeDependency(other *Transaction) {
	delete(tx.dependencies, other.TxID)
}

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
