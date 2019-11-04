package dependency

import (
	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"testing"
)

func Test_SingleBlock_SingleTX_SingleKeyRead(t *testing.T) {
	txID := "txID"

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}}}
	rawBlock := createBlock(2, [][]byte{createTxBytes(txID, "chaincode", reads, nil)})

	analyzer := NewAnalyzer(make(chan *Transaction, 500))
	if err := analyzer.Analyze(cached.WrapBlock(rawBlock)); err != nil {
		t.Error(err)
	}
	txs := analyzer.EmitUnblockedTxs()

	done := false
	for i := 0; ; i++ {
		select {
		case tx := <-txs:
			if tx.TxID != txID {
				t.Errorf("Wrong txID. Expected %v, got %v", txID, tx.TxID)
			}
		default:
			if i != 1 {
				t.Errorf("There were %d transactions in the channel, expected 1", i)
			}
			done = true
		}
		if done {
			break
		}
	}
}

func Test_SingleBlock_SingleTX_SingleKeyReadWrite(t *testing.T) {
	txID := "txID"

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")}}
	rawBlock := createBlock(2, [][]byte{createTxBytes(txID, "chaincode", reads, writes)})

	analyzer := NewAnalyzer(make(chan *Transaction, 500))
	if err := analyzer.Analyze(cached.WrapBlock(rawBlock)); err != nil {
		t.Error(err)
	}
	txs := analyzer.EmitUnblockedTxs()

	done := false
	for i := 0; ; i++ {
		select {
		case tx := <-txs:
			if tx.TxID != txID {
				t.Errorf("Wrong txID. Expected %v, got %v", txID, tx.TxID)
			}
		default:
			if i != 1 {
				t.Errorf("There were %d transactions in the channel, expected 1", i)
			}
			done = true
		}
		if done {
			break
		}
	}
}

func Test_SingleBlock_SingleTX_MultipleKeys(t *testing.T) {
	txID := "txID"

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")}}
	rawBlock := createBlock(2, [][]byte{createTxBytes(txID, "chaincode", reads, writes)})

	analyzer := NewAnalyzer(make(chan *Transaction, 500))
	if err := analyzer.Analyze(cached.WrapBlock(rawBlock)); err != nil {
		t.Error(err)
	}
	txs := analyzer.EmitUnblockedTxs()

	done := false
	for i := 0; ; i++ {
		select {
		case tx := <-txs:
			if tx.TxID != txID {
				t.Errorf("Wrong txID. Expected %v, got %v", txID, tx.TxID)
			}
		default:
			if i != 1 {
				t.Errorf("There were %d transactions in the channel, expected 1", i)
			}
			done = true
		}
		if done {
			break
		}
	}
}

func Test_SingleBlock_TwoTxs_NoConflict(t *testing.T) {
	txIDs := []string{"txID1", "txID2"}

	reads1 := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes1 := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")}}
	reads2 := []*kvrwset.KVRead{{Key: "key3", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key4", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes2 := []*kvrwset.KVWrite{{Key: "key3", Value: []byte("value1")},
		{Key: "key4", Value: []byte("value2")}}
	rawBlock := createBlock(2,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", reads1, writes1),
			createTxBytes(txIDs[1], "chaincode", reads2, writes2)})

	analyzer := NewAnalyzer(make(chan *Transaction, 500))
	if err := analyzer.Analyze(cached.WrapBlock(rawBlock)); err != nil {
		t.Error(err)
	}
	txs := analyzer.EmitUnblockedTxs()

	done := false
	for i := 0; ; i++ {
		select {
		case tx := <-txs:
			if tx.TxID != txIDs[i] {
				t.Errorf("Wrong txID. Expected %v, got %v", txIDs[i], tx.TxID)
			}
		default:
			if i != 2 {
				t.Errorf("There were %d transactions in the channel, expected 1", i)
			}
			done = true
		}
		if done {
			break
		}
	}
}

func Test_SingleBlock_TwoTxs_SecondBlocked(t *testing.T) {
	txIDs := []string{"txID1", "txID2"}

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")}}
	rawBlock := createBlock(2,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", reads, writes),
			createTxBytes(txIDs[1], "chaincode", reads, writes)})

	analyzer := NewAnalyzer(make(chan *Transaction, 500))
	if err := analyzer.Analyze(cached.WrapBlock(rawBlock)); err != nil {
		t.Error(err)
	}
	txs := analyzer.EmitUnblockedTxs()

	done := false
	for i := 0; ; i++ {
		select {
		case tx := <-txs:
			if tx.TxID != txIDs[i] {
				t.Errorf("Wrong txID. Expected %v, got %v", txIDs[i], tx.TxID)
			}
		default:
			if i != 1 {
				t.Errorf("There were %d transactions in the channel, expected 1", i)
			}
			done = true
		}
		if done {
			break
		}
	}
}

func Test_SingleBlock_TwoTxs_SecondFreedAfterFirstCommitted(t *testing.T) {
	txIDs := []string{"txID1", "txID2"}

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")}}
	rawBlock := createBlock(2,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", reads, writes),
			createTxBytes(txIDs[1], "chaincode", reads, writes)})

	analyzer := NewAnalyzer(make(chan *Transaction, 500))
	if err := analyzer.Analyze(cached.WrapBlock(rawBlock)); err != nil {
		t.Error(err)
	}
	txs := analyzer.EmitUnblockedTxs()

	done := false
	var tx *Transaction
	for i := 0; ; i++ {
		select {
		case tx = <-txs:
			if tx.TxID != txIDs[i] {
				t.Errorf("Wrong txID. Expected %v, got %v", txIDs[i], tx.TxID)
			}
		default:
			if i != 1 {
				t.Errorf("There were %d transactions in the channel, expected 1", i)
			}
			done = true
		}
		if done {
			break
		}
	}
	if tx == nil {
		return
	}
	tx.Committed()

	done = false
	for i := 1; ; i++ {
		select {
		case tx = <-txs:
			if tx.TxID != txIDs[i] {
				t.Errorf("Wrong txID. Expected %v, got %v", txIDs[i], tx.TxID)
			}
		default:
			if i != 2 {
				t.Errorf("There were %d transactions in the channel, expected 2", i)
			}
			done = true
		}
		if done {
			break
		}
	}

}

func Test_TxWithoutKeys(t *testing.T) {
	txIDs := []string{"txID1", "txID2", "txID3"}

	rawBlock := createBlock(2,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", []*kvrwset.KVRead{}, []*kvrwset.KVWrite{}),
			createTxBytes(txIDs[1], "chaincode", []*kvrwset.KVRead{}, []*kvrwset.KVWrite{}),
			createTxBytes(txIDs[2], "chaincode", []*kvrwset.KVRead{}, []*kvrwset.KVWrite{})})

	analyzer := NewAnalyzer(make(chan *Transaction, 500))
	if err := analyzer.Analyze(cached.WrapBlock(rawBlock)); err != nil {
		t.Error(err)
	}
	txs := analyzer.EmitUnblockedTxs()

	done := false
	var tx *Transaction
	for i := 0; ; i++ {
		select {
		case tx = <-txs:
			if tx.TxID != txIDs[i] {
				t.Errorf("Wrong txID. Expected %v, got %v", txIDs[i], tx.TxID)
			}
		default:
			if i != 3 {
				t.Errorf("There were %d transactions in the channel, expected 3", i)
			}
			done = true
		}
		if done {
			break
		}
	}
}

func Test_MultipleBlocks_OutOfOrder(t *testing.T) {
	txIDs := []string{"txID1", "txID2", "txID3", "txID4", "txID5", "txID6"}

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key3", Value: []byte("value2")},
		{Key: "key4", Value: []byte("value4")}}
	rawBlock1 := createBlock(2,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", reads, writes),
			createTxBytes(txIDs[1], "chaincode", reads, writes)})

	rawBlock2 := createBlock(3,
		[][]byte{
			createTxBytes(txIDs[2], "chaincode", reads, writes),
			createTxBytes(txIDs[3], "chaincode", reads, writes)})

	rawBlock3 := createBlock(4,
		[][]byte{
			createTxBytes(txIDs[4], "chaincode", reads, writes),
			createTxBytes(txIDs[5], "chaincode", reads, writes)})

	analyzer := NewAnalyzer(make(chan *Transaction, 500))
	if err := analyzer.Analyze(cached.WrapBlock(rawBlock1)); err != nil {
		t.Error(err)
	}
	txs := analyzer.EmitUnblockedTxs()

	done := false
	var tx *Transaction
	for i := 0; ; i++ {
		select {
		case tx = <-txs:
			if tx.TxID != txIDs[i] {
				t.Errorf("Wrong txID. Expected %v, got %v", txIDs[i], tx.TxID)
			}
			tx.Committed()
		default:
			if i != 2 {
				t.Errorf("There were %d transactions in the channel, expected 2", i)
			}
			done = true
		}
		if done {
			break
		}
	}

	if err := analyzer.Analyze(cached.WrapBlock(rawBlock3)); err != nil {
		t.Error(err)
	}

	done = false
	for i := 2; ; i++ {
		select {
		case tx = <-txs:
			if tx.TxID != txIDs[i] {
				t.Errorf("Wrong txID. Expected %v, got %v", txIDs[i], tx.TxID)
			}
			tx.Committed()
		default:
			if i != 2 {
				t.Errorf("There were %d transactions in the channel, expected 0", i)
			}
			done = true
		}
		if done {
			break
		}
	}

	if err := analyzer.Analyze(cached.WrapBlock(rawBlock2)); err != nil {
		t.Error(err)
	}
	done = false
	for i := 2; ; i++ {
		select {
		case tx = <-txs:
			if i >= len(txIDs) {
				t.Errorf("No other transaction expected, got %v", tx.TxID)
				continue
			}
			if tx.TxID != txIDs[i] {
				t.Errorf("Wrong txID. Expected %v, got %v", txIDs[i], tx.TxID)
			}
			tx.Committed()
		default:
			if i != 6 {
				t.Errorf("There were %d transactions in the channel, expected 4", i-2)
			}
			done = true
		}
		if done {
			break
		}
	}
}

func createBlock(blockNum uint64, txs [][]byte) *common.Block {
	return &common.Block{Header: &common.BlockHeader{Number: blockNum}, Data: &common.BlockData{Data: txs}}
}

func createTxBytes(txID string, chaincodeName string, reads []*kvrwset.KVRead, writes []*kvrwset.KVWrite) []byte {
	set := &rwsetutil.TxRwSet{
		NsRwSets: []*rwsetutil.NsRwSet{{
			NameSpace: chaincodeName,
			KvRwSet: &kvrwset.KVRWSet{
				Reads:  reads,
				Writes: writes}}}}
	pb, _ := set.ToProtoBytes()
	pb, _ = proto.Marshal(&peer.ChaincodeAction{Results: pb})
	pb, _ = proto.Marshal(&peer.ProposalResponsePayload{Extension: pb})
	pb, _ = proto.Marshal(&peer.ChaincodeActionPayload{Action: &peer.ChaincodeEndorsedAction{ProposalResponsePayload: pb}})
	pb, _ = proto.Marshal(&peer.Transaction{Actions: []*peer.TransactionAction{{Payload: pb}}})
	pb2, _ := proto.Marshal(&common.ChannelHeader{TxId: txID, Type: int32(common.HeaderType_ENDORSER_TRANSACTION)})
	pb, _ = proto.Marshal(&common.Payload{Data: pb, Header: &common.Header{ChannelHeader: pb2}})
	pb, _ = proto.Marshal(&common.Envelope{Payload: pb})
	return pb
}
