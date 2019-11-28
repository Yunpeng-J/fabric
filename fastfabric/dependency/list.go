package dependency

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
)

type TxNode struct {
	elements   map[string]*TxElement
	next       *TxNode
	prev       *TxNode
	skipNext   *TxNode
	skipPrev   *TxNode
	rangeStart *version.Height
	rangeEnd   *version.Height
	isWrite    bool
}

type TxElement struct {
	*Transaction
	parentNode *TxNode
}

type skipList struct {
	root *TxNode
	len  int
	txs  map[string][]*TxElement
}

func NewSkipList() *skipList {
	return &skipList{len: 0, txs: map[string][]*TxElement{}, root: &TxNode{
		rangeStart: version.NewHeight(0, 0),
		rangeEnd:   version.NewHeight(0, 0),
		isWrite:    true,
	}}
}

func (l *skipList) Delete(txID string) {
	elements, ok := l.txs[txID]
	if !ok {
		return
	}
	for _, element := range elements {
		node := element.parentNode
		node.removeElement(element)
		if len(node.elements) == 0 {
			node.removeFromSkip()
			l.removeNode(node)
		}

		l.len -= 1
	}
	delete(l.txs, txID)
}

func (n *TxNode) removeElement(e *TxElement) {
	if n.next != nil {
		for _, e := range n.next.elements {
			e.removeDependency(e.Transaction)
		}
	}
	e.parentNode = nil
	delete(n.elements, e.TxID)
}

func (n *TxNode) removeFromSkip() {
	if n.skipNext == n.next {
		linkSkip(n.skipPrev, n.next)
	} else {
		n.substituteSkipWith(n.next)
	}
}

func (l *skipList) removeNode(node *TxNode) {
	link(node.prev, node.next)

	if node.prev == nil {
		l.root = node.next
	}
}

func (l *skipList) AddRead(tx *Transaction) {
	l.add(tx, false)
}

func (l *skipList) AddWrite(tx *Transaction) {
	l.add(tx, true)

}

func (l *skipList) add(tx *Transaction, isWrite bool) {
	skipClosest, closest := l.findClosestNode(tx)
	var e *TxElement
	if isWrite {
		e = closest.appendNewNode(tx, isWrite)
	} else {
		switch {
		case !closest.isWrite:
			e = closest.add(tx)
			break
		case closest.next != nil && !closest.next.isWrite:
			e = closest.next.add(tx)
			break
		default:
			e = closest.appendNewNode(tx, isWrite)
		}
	}
	skipClosest.updateSkips(e.parentNode)
	e.addDependencies()

	l.len += 1
	l.txs[tx.TxID] = append(l.txs[tx.TxID], e)
}

func (n *TxNode) substituteSkipWith(newNode *TxNode) {
	linkSkip(n.skipPrev, newNode)
	linkSkip(newNode, n.skipNext)
	n.skipNext = nil
	n.skipPrev = nil
}

func (this *TxElement) addDependencies() {
	n := this.parentNode
	if n.next != nil {
		for _, nextTx := range n.next.elements {
			nextTx.addDependency(this.Transaction)
		}
	}
	if n.prev != nil {
		for _, prevTx := range n.prev.elements {
			this.addDependency(prevTx.Transaction)
		}
	}
}

func (l *skipList) findClosestNode(tx *Transaction) (skipClosest, closest *TxNode) {
	skipClosest = l.root
	for skipClosest != nil && skipClosest.skipNext != nil && skipClosest.skipNext.rangeStart.Compare(tx.Version) <= 0 {
		skipClosest = skipClosest.skipNext
	}

	closest = skipClosest
	for closest != nil && closest.next != nil && closest.next.rangeStart.Compare(tx.Version) <= 0 {
		closest = closest.next
	}

	return skipClosest, closest
}

func (l *skipList) First() *TxNode {
	return l.root.next
}

func (n *TxNode) appendNewNode(tx *Transaction, isWrite bool) *TxElement {
	newElem := &TxElement{Transaction: tx}
	newNode := &TxNode{
		elements:   map[string]*TxElement{tx.TxID: newElem},
		rangeStart: tx.Version,
		rangeEnd:   tx.Version,
		isWrite:    isWrite}
	newElem.parentNode = newNode

	if n.rangeEnd.Compare(newNode.rangeStart) <= 0 {
		link(newNode, n.next)
		link(n, newNode)
	} else {
		n1, n2 := n.split(newNode.rangeStart)
		link(n1, newNode)
		link(newNode, n2)
	}

	return newElem
}

func (n *TxNode) add(tx *Transaction) *TxElement {
	newElem := &TxElement{Transaction: tx, parentNode: n}
	n.elements[tx.TxID] = newElem
	if n.rangeStart.Compare(tx.Version) > 0 {
		n.rangeStart = tx.Version
	}
	if n.rangeEnd.Compare(tx.Version) < 0 {
		n.rangeEnd = tx.Version
	}
	return newElem
}

func (n *TxNode) updateSkips(newNode *TxNode) {
	switch {
	case n.rangeStart.BlockNum == newNode.rangeStart.BlockNum:
		return
	case n.skipNext == nil:
		linkSkip(n, newNode)
		return
	case n.skipNext.rangeStart.BlockNum == newNode.rangeStart.BlockNum:
		n.skipNext.substituteSkipWith(newNode)
		return
	default:
		linkSkip(newNode, n.skipNext)
		linkSkip(n, newNode)
	}
}

func (n *TxNode) split(height *version.Height) (*TxNode, *TxNode) {
	newNode := &TxNode{
		next:     n.next,
		prev:     n,
		rangeEnd: n.rangeEnd,
		elements: make(map[string]*TxElement),
		isWrite:  n.isWrite}

	n.rangeEnd = nil
	for txId, e := range n.elements {
		if e.Version.Compare(height) > 0 {
			newNode.elements[txId] = e
			e.parentNode = newNode
			if newNode.rangeStart == nil || newNode.rangeStart.Compare(e.Version) > 0 {
				newNode.rangeStart = e.Version
			}
		} else {
			if n.rangeEnd == nil || n.rangeEnd.Compare(e.Version) < 0 {
				n.rangeEnd = e.Version
			}
		}
	}

	for txId, _ := range newNode.elements {
		delete(n.elements, txId)
	}

	return n, newNode
}

func (this *TxNode) setPrev(prev *TxNode) {
	defer func() { this.prev = prev }()
	if prev != nil {
		for _, thisElement := range this.elements {
			newDependencies := map[string]*Transaction{}
			for _, prevElement := range prev.elements {
				if prevElement.TxID == thisElement.TxID {
					return
				}
				newDependencies[prevElement.TxID] = prevElement.Transaction
			}
			<-thisElement.dependencies
			thisElement.dependencies <- newDependencies
		}

	}
}

func linkSkip(n1 *TxNode, n2 *TxNode) {
	if n1 != nil {
		n1.skipNext = n2
	}
	if n2 != nil {
		n2.skipPrev = n1
	}
}

func link(n1 *TxNode, n2 *TxNode) {
	if n1 != nil {
		n1.next = n2
	}
	if n2 != nil {
		n2.setPrev(n1)
	}
}
