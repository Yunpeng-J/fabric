package dependency

import "github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"

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
	Value *Transaction
	node  *TxNode
}

type skipList struct {
	root *TxNode
	len  int
	txs  map[string][]*TxElement
}

func NewSkipList() *skipList {
	return &skipList{len: 0, txs: make(map[string][]*TxElement)}
}

func (l *skipList) Delete(txID string) {
	tx, ok := l.txs[txID]
	if !ok {
		return
	}
	for _, e := range tx {
		node := e.node
		node.removeElement(e)
		if len(node.elements) == 0 {
			if node.skipNext != nil {
				node.removeFromSkip()
			}

			l.removeNode(node)
		}

		l.len -= 1
	}
	delete(l.txs, txID)
}

func (n *TxNode) removeElement(e *TxElement) {
	e.node = nil
	delete(n.elements, e.Value.TxID)
}

func (n *TxNode) removeFromSkip() {
	if n.skipNext != n.next {
		linkSkip(n.skipPrev, n.skipNext)
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
	var e *TxElement
	if l.len == 0 {
		e = l.addToEmpty(tx, isWrite)
	} else {
		e = l.addToNonEmpty(tx, isWrite)
	}
	l.len += 1
	l.txs[tx.TxID] = append(l.txs[tx.TxID], e)
}

func (l *skipList) addToNonEmpty(tx *Transaction, isWrite bool) *TxElement {
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
	skipClosest.updateSkips(e.node)
	e.addDependencies()

	return e
}

func (n *TxNode) substituteSkipWith(newNode *TxNode) {
	linkSkip(n.skipPrev, newNode)
	linkSkip(newNode, n.skipNext)
	n.skipNext = nil
	n.skipPrev = nil
}

func (this *TxElement) addDependencies() {
	n := this.node
	if n.next != nil {
		for _, e := range n.next.elements {
			e.Value.addDependence(this.Value)
		}
	}
	if n.prev != nil {
		for _, e := range n.prev.elements {
			this.Value.addDependence(e.Value)
		}
	}
}

func (l *skipList) addToEmpty(tx *Transaction, isWrite bool) *TxElement {
	e := &TxElement{Value: tx}
	rootNode := &TxNode{
		elements:   map[string]*TxElement{tx.TxID: e},
		rangeStart: tx.Version,
		rangeEnd:   tx.Version,
		isWrite:    isWrite}
	e.node = rootNode
	l.root = rootNode
	return e
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

func (n *TxNode) appendNewNode(tx *Transaction, isWrite bool) *TxElement {
	newElem := &TxElement{Value: tx}
	newNode := &TxNode{
		elements:   map[string]*TxElement{tx.TxID: newElem},
		rangeStart: tx.Version,
		rangeEnd:   tx.Version,
		isWrite:    isWrite}
	newElem.node = newNode

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
	newElem := &TxElement{Value: tx, node: n}
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
		if e.Value.Version.Compare(height) > 0 {
			newNode.elements[txId] = e
			e.node = newNode
			if newNode.rangeStart == nil || newNode.rangeStart.Compare(e.Value.Version) > 0 {
				newNode.rangeStart = e.Value.Version
			}
		} else {
			if n.rangeEnd == nil || n.rangeEnd.Compare(e.Value.Version) < 0 {
				n.rangeEnd = e.Value.Version
			}
		}
	}

	for txId, _ := range newNode.elements {
		delete(n.elements, txId)
	}

	return n, newNode
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
		n2.prev = n1
	}
}
