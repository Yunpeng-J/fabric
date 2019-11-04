package dependency

import (
	"fmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/protos/common"
	"strings"
	"sync"
)

var compositeKeySep = string([]byte{0x00})

type analyzer struct {
	keyDPs                 map[string]*skipList
	keyMutex               sync.RWMutex
	currentKnowledgeHeight *version.Height
	currentBlockLength     uint64
	knowledgeStash         *stash
	unblockedTxs           chan *Transaction
	knowledgeChangedEvents map[string]func(knowledge *version.Height)
	knowledgeMutex         sync.RWMutex
}

type Analyzer interface {
	Analyze(block *cached.Block) error
	EmitUnblockedTxs() chan *Transaction
}

func NewAnalyzer(output chan *Transaction) Analyzer {
	return &analyzer{
		knowledgeStash:         &stash{store: make(map[uint64][]bool)},
		keyDPs:                 make(map[string]*skipList),
		knowledgeChangedEvents: make(map[string]func(knowledge *version.Height)),
		unblockedTxs:           output,
		keyMutex:               sync.RWMutex{},
		knowledgeMutex:         sync.RWMutex{}}
}

func (a *analyzer) Analyze(b *cached.Block) error {
	blockNum := b.Header.Number
	fmt.Println("analyzing block", blockNum)
	_ = b.UnmarshalAll()
	envs, err := b.UnmarshalAllEnvelopes()
	if err != nil {
		return err
	}

	var txNum int
	var env *cached.Envelope
	for txNum, env = range envs {
		fmt.Printf("Analyzing tx no. %d\n", txNum)

		pl, err := env.UnmarshalPayload()
		if err != nil {
			return err
		}

		t, err := NewTransaction(blockNum, uint64(txNum), pl)
		if err != nil {
			return err
		}

		t.OnNoDependence(a.registerNotification)
		t.OnAddedDependence(a.cancelNotification)
		t.OnUnblocked(a.addToOutput)
		t.OnCommitted(a.removeDependencies)
		if t.RwSet != nil {
			a.addDependencies(t)
		}
		fmt.Printf("Dependency count for tx %d: %d\n", txNum, len(t.dependencies))
		if len(t.dependencies) != 0 {
			for _, tx := range t.dependencies {
				fmt.Println("current tx:" + t.TxID)
				for _, ns := range t.RwSet.NsRwSets {
					for _, r := range ns.KvRwSet.Reads {
						fmt.Println(r.Key)
					}
					for _, w := range ns.KvRwSet.Writes {
						fmt.Println(w.Key)
					}
				}
				fmt.Println("dependency:" + tx.TxID)
				for _, ns := range tx.RwSet.NsRwSets {
					for _, r := range ns.KvRwSet.Reads {
						fmt.Println(r.Key)
					}
					for _, w := range ns.KvRwSet.Writes {
						fmt.Println(w.Key)
					}
				}
			}
		}
		if a.updateKnowledgeHeight(t.Version, uint64(len(envs))) {
			a.notify()
		}
		fmt.Printf("Current knowledge height:%v\n", a.currentKnowledgeHeight)
	}

	return nil
}

func (a *analyzer) addDependencies(tx *Transaction) {
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			if strings.HasPrefix(w.Key, "oracle_") {
				continue
			}
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			a.keyMutex.RLock()
			list, ok := a.keyDPs[compKey]
			a.keyMutex.RUnlock()
			if !ok {
				list = NewSkipList()
				a.keyMutex.Lock()
				a.keyDPs[compKey] = list
				a.keyMutex.Unlock()
			}
			list.AddWrite(tx)
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			a.keyMutex.RLock()
			list, ok := a.keyDPs[compKey]
			a.keyMutex.RUnlock()
			if !ok {
				list = NewSkipList()
				a.keyMutex.Lock()
				a.keyDPs[compKey] = list
				a.keyMutex.Unlock()
			}
			list.AddRead(tx)
		}
	}
}

func (a *analyzer) removeDependencies(tx *Transaction) {
	if tx.RwSet == nil {
		return
	}
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			a.keyMutex.RLock()
			if list, ok := a.keyDPs[compKey]; ok {
				list.Delete(tx.TxID)
			}
			a.keyMutex.RUnlock()
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			a.keyMutex.RLock()
			if list, ok := a.keyDPs[compKey]; ok {
				list.Delete(tx.TxID)
			}
			a.keyMutex.RUnlock()
		}
	}
}

func (a *analyzer) changeDependencies(tx *Transaction, change func(string)) {
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			change(compKey)
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			change(compKey)
		}
	}
}

func (a *analyzer) EmitUnblockedTxs() chan *Transaction {
	return a.unblockedTxs
}

func (a *analyzer) addToOutput(tx *Transaction) {
	fmt.Println("Added to output")
	a.unblockedTxs <- tx
	a.cancelNotification(tx)
}

func (a *analyzer) registerNotification(tx *Transaction) {
	a.OnKnowledgeHeightChange(tx.TxID, tx.CheckStatus)
	if a.currentKnowledgeHeight != nil {
		tx.CheckStatus(a.currentKnowledgeHeight)
	}
}

func (a *analyzer) cancelNotification(tx *Transaction) {
	a.knowledgeMutex.Lock()
	delete(a.knowledgeChangedEvents, tx.TxID)
	a.knowledgeMutex.Unlock()
}

func (a *analyzer) updateKnowledgeHeight(newtxHeight *version.Height, txBlockLen uint64) (changed bool) {
	if a.currentKnowledgeHeight == nil {
		a.currentKnowledgeHeight = newtxHeight
		a.currentBlockLength = txBlockLen
		return true
	}

	isContinuousInSameBlock :=
		a.currentKnowledgeHeight.BlockNum == newtxHeight.BlockNum &&
			a.currentKnowledgeHeight.TxNum+1 == newtxHeight.TxNum

	isContinuousInNextBlock :=
		a.currentKnowledgeHeight.BlockNum+1 == newtxHeight.BlockNum &&
			newtxHeight.TxNum == 0 &&
			a.currentKnowledgeHeight.TxNum+1 == a.currentBlockLength

	if isContinuousInSameBlock || isContinuousInNextBlock {
		a.currentKnowledgeHeight = newtxHeight
		a.currentBlockLength = txBlockLen
		a.updateKnowledgeHeightFromStash()
		return true
	}
	a.knowledgeStash.add(newtxHeight, txBlockLen)
	return false
}

func (a *analyzer) updateKnowledgeHeightFromStash() {
	blockNum := a.currentKnowledgeHeight.BlockNum
	if a.currentKnowledgeHeight.TxNum+1 == a.currentBlockLength {
		blockNum += 1
	}
	for stashHeight, blockLength := a.knowledgeStash.popContinousEntries(a.currentKnowledgeHeight); stashHeight != nil; blockNum++ {
		a.currentKnowledgeHeight = stashHeight
		a.currentBlockLength = blockLength
	}
}

func (a *analyzer) notify() {
	a.knowledgeMutex.RLock()
	for _, notify := range a.knowledgeChangedEvents {
		a.knowledgeMutex.RUnlock()
		notify(a.currentKnowledgeHeight)
		a.knowledgeMutex.RLock()
	}
	a.knowledgeMutex.RUnlock()
}

func (a *analyzer) OnKnowledgeHeightChange(txId string, event func(height *version.Height)) {
	a.knowledgeMutex.Lock()
	a.knowledgeChangedEvents[txId] = event
	a.knowledgeMutex.Unlock()
}

type Transaction struct {
	Version      *version.Height
	Payload      *cached.Payload
	TxID         string
	dependencies map[string]*Transaction

	committedEvents         []func(tx *Transaction)
	unblockedEvent          func(this *Transaction)
	addedDependenceEvent    func(tx *Transaction)
	noDependenciesEvent     func(tx *Transaction)
	getsUpdateNotifications bool
	RwSet                   *cached.TxRwSet
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
			TxID:            chdr.TxId,
			Payload:         payload,
			RwSet:           nil,
			dependencies:    make(map[string]*Transaction),
			committedEvents: make([]func(transaction *Transaction), 0)}, nil
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
		TxID:            chdr.TxId,
		Payload:         payload,
		RwSet:           rwset,
		dependencies:    make(map[string]*Transaction),
		committedEvents: make([]func(transaction *Transaction), 0)}, nil

}

func (tx *Transaction) OnAddedDependence(addedDependenceEvent func(*Transaction)) {
	tx.addedDependenceEvent = addedDependenceEvent
}

func (tx *Transaction) OnNoDependence(noDependenceEvent func(*Transaction)) {
	tx.noDependenciesEvent = noDependenceEvent
	//This step checks if there are currently no dependencies
	tx.checkDependencyCount()
}

func (tx *Transaction) OnCommitted(committedEvent func(*Transaction)) {
	tx.committedEvents = append(tx.committedEvents, committedEvent)
}

func (tx *Transaction) OnUnblocked(unblockedEvent func(*Transaction)) {
	tx.unblockedEvent = unblockedEvent
}

func (tx *Transaction) Committed() {
	fmt.Println("Transaction committed")
	for _, emit := range tx.committedEvents {
		emit(tx)
	}
	fmt.Println("all events fired")
}

func (tx *Transaction) CheckStatus(knowledge *version.Height) {
	if tx.Version != nil && tx.Version.Compare(knowledge) >= 0 && len(tx.dependencies) == 0 {
		tx.unblock()
	}
}

func (tx *Transaction) unblock() {
	if tx.unblockedEvent != nil {
		tx.unblockedEvent(tx)
	}
}

func (tx *Transaction) addDependence(other *Transaction) {
	if _, ok := tx.dependencies[other.TxID]; other == tx || ok {
		return
	}

	tx.dependencies[other.TxID] = other
	if tx.addedDependenceEvent != nil {
		tx.addedDependenceEvent(tx)
	}

	other.OnCommitted(tx.removeDependence)
}

func (tx *Transaction) removeDependence(other *Transaction) {
	delete(tx.dependencies, other.TxID)
	tx.checkDependencyCount()
}

func (tx *Transaction) checkDependencyCount() {
	if len(tx.dependencies) == 0 && tx.noDependenciesEvent != nil {
		tx.noDependenciesEvent(tx)
	}
}

type stash struct {
	store map[uint64][]bool
}

func (s *stash) add(txHeight *version.Height, blockLen uint64) {
	if _, ok := s.store[txHeight.BlockNum]; !ok {
		s.store[txHeight.BlockNum] = make([]bool, blockLen)
	}
	s.store[txHeight.BlockNum][txHeight.TxNum] = true
}

func (s *stash) popContinousEntries(prevHeight *version.Height) (height *version.Height, blockLength uint64) {
	blockNum := prevHeight.BlockNum
	txNum := prevHeight.TxNum + 1

	var foundEntries bool
	for {
		block, ok := s.store[blockNum]
		if !ok {
			break
		}

		txNum, foundEntries = scanBlock(block, txNum)
		if !foundEntries {
			break
		}

		height = &version.Height{BlockNum: blockNum, TxNum: txNum}
		blockLength = uint64(len(block))
		if txNum+1 == blockLength {
			delete(s.store, blockNum)
			blockNum += 1
			txNum = 0
		} else {
			break
		}
	}

	return height, blockLength
}

func scanBlock(block []bool, txNum uint64) (uint64, bool) {
	i := int(txNum)
	for ; i < len(block); i++ {
		if !block[i] {
			i -= 1
			break
		}
	}

	return uint64(i), i >= 0
}

func constructCompositeKey(ns string, key string) string {
	var buffer strings.Builder
	buffer.WriteString(ns)
	buffer.WriteString(compositeKeySep)
	buffer.WriteString(key)
	return buffer.String()
}
