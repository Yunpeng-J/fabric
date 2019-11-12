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
	currentWatermark   chan *version.Height
	currentBlockLength chan uint64
	futureKnowledge    chan *blockFlags
	unblockedTxs       chan map[string]*Transaction
	outputPerBlock     chan map[uint64]chan *Transaction
	blockedTxsPerBlock chan map[uint64]int
	committedTxs       chan *Transaction
	done               chan bool
	committingTxs      chan map[string]*Transaction
	once               sync.Once
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
		currentWatermark:   make(chan *version.Height, 1),
		currentBlockLength: make(chan uint64, 1),
		futureKnowledge:    make(chan *blockFlags, 1),
		unblockedTxs:       make(chan map[string]*Transaction, 1),
		outputPerBlock:     make(chan map[uint64]chan *Transaction, 1),
		committedTxs:       make(chan *Transaction, 1000),
		done:               make(chan bool),
		committingTxs:      make(chan map[string]*Transaction, 1),
		blockedTxsPerBlock: make(chan map[uint64]int, 1),
	}
	a.keyDPs <- make(map[string]chan *skipList)
	a.currentWatermark <- &version.Height{
		BlockNum: 0,
		TxNum:    0,
	}
	a.currentBlockLength <- 1
	a.futureKnowledge <- &blockFlags{store: make(map[uint64][]bool)}
	a.unblockedTxs <- make(map[string]*Transaction)
	a.outputPerBlock <- make(map[uint64]chan *Transaction)
	a.committingTxs <- make(map[string]*Transaction)
	a.blockedTxsPerBlock <- make(map[uint64]int)
	go func() {
		for {
			select {
			case _, more := <-a.done:
				if !more {
					return
				}
			case committedTx := <-a.committedTxs:
				a.removeDependencies(committedTx)
			default:
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

	var txs []*Transaction
	for txNum, env := range envs {
		pl, err := env.UnmarshalPayload()
		if err != nil {
			return nil, err
		}

		t, err := NewTransaction(blockNum, uint64(txNum), pl)
		if err != nil {
			return nil, err
		}
		txs = append(txs, t)
	}

	output := make(chan *Transaction, len(envs))
	opb := <-a.outputPerBlock
	opb[blockNum] = output
	a.outputPerBlock <- opb
	btx := <-a.blockedTxsPerBlock
	btx[blockNum] = len(txs)
	a.blockedTxsPerBlock <- btx

	for _, t := range txs {
		go func(t *Transaction) {
			if t.RwSet != nil {
				a.addDependencies(t)
			} else {
				btx := <-a.blockedTxsPerBlock
				btx[blockNum] -= 1
				output <- t
				if btx[blockNum] == 0 {
					close(output)
				}
				a.blockedTxsPerBlock <- btx
			}

			if a.updateWatermark(t.Version, uint64(len(envs))) {
				a.notifyTxs()
			}
		}(t)
	}

	return output, nil
}

func (a *analyzer) addDependencies(tx *Transaction) {
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			if strings.HasPrefix(w.Key, "oracle_") {
				continue
			}
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			list := <-a.getDPListForKey(compKey)
			list.AddWrite(tx)
			kdp := <-a.keyDPs
			kdp[compKey] <- list
			a.keyDPs <- kdp
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			list := <-a.getDPListForKey(compKey)
			list.AddRead(tx)
			kdp := <-a.keyDPs
			kdp[compKey] <- list
			a.keyDPs <- kdp
		}
	}
	a.tryToUnblock(tx)
}

func (a *analyzer) tryToUnblock(tx *Transaction) {
	if len(tx.dependencies) > 0 {
		ut := <-a.unblockedTxs
		delete(ut, tx.TxID)
		a.unblockedTxs <- ut
		return
	}
	wm := <-a.currentWatermark
	defer func() { a.currentWatermark <- wm }()

	if wm == nil || wm.Compare(tx.Version) < 0 {
		ut := <-a.unblockedTxs
		ut[tx.TxID] = tx
		a.unblockedTxs <- ut
		return
	}

	ut := <-a.unblockedTxs
	delete(ut, tx.TxID)
	a.unblockedTxs <- ut
	ctx := <-a.committingTxs
	if _, ok := ctx[tx.TxID]; !ok {
		ctx[tx.TxID] = tx
		opb := <-a.outputPerBlock
		opb[tx.Version.BlockNum] <- tx
		btx := <-a.blockedTxsPerBlock
		btx[tx.Version.BlockNum] -= 1
		if btx[tx.Version.BlockNum] == 0 {
			close(opb[tx.Version.BlockNum])
		}
		a.blockedTxsPerBlock <- btx
		a.outputPerBlock <- opb
	}
	a.committingTxs <- ctx
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
			kdp := <-a.keyDPs
			listChan, ok := kdp[compKey]
			a.keyDPs <- kdp
			if ok {
				list := <-listChan
				list.Delete(tx.TxID)
				first := list.First()
				if first != nil {
					for _, element := range first.elements {
						a.tryToUnblock(element.Transaction)
					}
				}
				kdp[compKey] <- list
			}
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			kdp := <-a.keyDPs
			list, ok := <-kdp[compKey]
			a.keyDPs <- kdp
			if ok {
				list.Delete(tx.TxID)
				first := list.First()
				if first != nil {
					for _, element := range first.elements {
						a.tryToUnblock(element.Transaction)
					}
				}
				kdp[compKey] <- list
			}
		}
	}
}

func (a *analyzer) updateWatermark(newtxHeight *version.Height, txBlockLen uint64) (changed bool) {
	cwm := <-a.currentWatermark
	cbl := <-a.currentBlockLength
	defer func() {
		a.currentWatermark <- cwm
		a.currentBlockLength <- cbl
	}()
	if cwm == nil {
		cwm = newtxHeight
		cbl = txBlockLen
		return true
	}

	isNextTxInSameBlock :=
		cwm.BlockNum == newtxHeight.BlockNum &&
			cwm.TxNum+1 == newtxHeight.TxNum

	isNextTxAftercompletedBlock :=
		cwm.BlockNum+1 == newtxHeight.BlockNum &&
			newtxHeight.TxNum == 0 &&
			cwm.TxNum+1 == cbl

	fk := <-a.futureKnowledge
	defer func() { a.futureKnowledge <- fk }()
	if isNextTxInSameBlock || isNextTxAftercompletedBlock {
		cwm = newtxHeight
		cbl = txBlockLen

		futureWatermark := *cwm

		if futureWatermark.TxNum+1 == cbl {
			futureWatermark.BlockNum += 1
			futureWatermark.TxNum = 0
		} else {
			futureWatermark.TxNum += 1
		}

		newWM, newBL := fk.popContinuousEntriesStartingWith(futureWatermark)
		if newWM != nil {
			cwm = newWM
			cbl = newBL
		}
		return true
	}
	fk.add(newtxHeight, txBlockLen)
	return false
}

func (a *analyzer) notifyTxs() {
	utx := <-a.unblockedTxs
	a.unblockedTxs <- utx
	for _, tx := range utx {
		a.tryToUnblock(tx)
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
			dependencies: make(map[string]*Transaction),
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
		dependencies: make(map[string]*Transaction)}, nil

}

func (tx *Transaction) addDependency(other *Transaction) {
	if _, ok := tx.dependencies[other.TxID]; other == tx || ok {
		return
	}

	tx.dependencies[other.TxID] = other
}

func (tx *Transaction) removeDependency(other *Transaction) {
	delete(tx.dependencies, other.TxID)
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

type blockFlags struct {
	store map[uint64][]bool
}

func (s *blockFlags) add(txHeight *version.Height, blockLen uint64) {
	if _, ok := s.store[txHeight.BlockNum]; !ok {
		s.store[txHeight.BlockNum] = make([]bool, blockLen)
	}
	s.store[txHeight.BlockNum][txHeight.TxNum] = true
}

func (s *blockFlags) popContinuousEntriesStartingWith(newHeight version.Height) (height *version.Height, blockLength uint64) {
	height = nil

	txNum := int(newHeight.TxNum)
	blockNum := newHeight.BlockNum
	for ; ; blockNum++ {
		block, ok := s.store[blockNum]
		if !ok {
			break
		}

		foundNum := -1
		for ; txNum < len(block); txNum++ {
			if block[txNum] {
				foundNum = txNum
			} else {
				break
			}
		}

		if txNum+1 == len(block) {
			delete(s.store, blockNum)
			txNum = 0
		}

		if foundNum == -1 {
			break
		}

		height = &version.Height{BlockNum: blockNum, TxNum: uint64(foundNum)}
		blockLength = uint64(len(block))
	}

	return height, blockLength
}

func constructCompositeKey(ns string, key string) string {
	var buffer strings.Builder
	buffer.WriteString(ns)
	buffer.WriteString(compositeKeySep)
	buffer.WriteString(key)
	return buffer.String()
}
