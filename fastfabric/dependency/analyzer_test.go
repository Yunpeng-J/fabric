package dependency

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"strconv"
	"sync"
	"testing"
)

func Test_SingleBlock_SingleTX_SingleKeyRead(t *testing.T) {
	txID := "txID"

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}}}
	rawBlock := createBlock(1, [][]byte{createTxBytes(txID, "chaincode", reads, nil)})

	analyzer := NewAnalyzer()
	txs, err := analyzer.Analyze(cached.WrapBlock(rawBlock))
	if err != nil {
		t.Error(err)
	}

	txCount := 0
	for tx := range txs {
		if tx.TxID != txID {
			t.Errorf("Wrong txID. Expected %v, got %v", txID, tx.TxID)
		}
		txCount++
	}

	if txCount != 1 {
		t.Errorf("There were %d transactions in the channel, expected 1", txCount)
	}
}

func Test_SingleBlock_SingleTX_SingleKeyReadWrite(t *testing.T) {
	txID := "txID"

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")}}
	rawBlock := createBlock(1, [][]byte{createTxBytes(txID, "chaincode", reads, writes)})

	analyzer := NewAnalyzer()
	txs, err := analyzer.Analyze(cached.WrapBlock(rawBlock))
	if err != nil {
		t.Error(err)
	}

	txCount := 0
	for tx := range txs {
		if tx.TxID != txID {
			t.Errorf("Wrong txID. Expected %v, got %v", txID, tx.TxID)
		}
		txCount++
	}

	if txCount != 1 {
		t.Errorf("There were %d transactions in the channel, expected 1", txCount)
	}
}

func Test_SingleBlock_SingleTX_MultipleKeys(t *testing.T) {
	txID := "txID"

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")}}
	rawBlock := createBlock(1, [][]byte{createTxBytes(txID, "chaincode", reads, writes)})

	analyzer := NewAnalyzer()
	txs, err := analyzer.Analyze(cached.WrapBlock(rawBlock))
	if err != nil {
		t.Error(err)
	}

	txCount := 0
	for tx := range txs {
		if tx.TxID != txID {
			t.Errorf("Wrong txID. Expected %v, got %v", txID, tx.TxID)
		}
		txCount++
	}

	if txCount != 1 {
		t.Errorf("There were %d transactions in the channel, expected 1", txCount)
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
	rawBlock := createBlock(1,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", reads1, writes1),
			createTxBytes(txIDs[1], "chaincode", reads2, writes2)})

	analyzer := NewAnalyzer()
	txs, err := analyzer.Analyze(cached.WrapBlock(rawBlock))
	if err != nil {
		t.Error(err)
	}

	resultTxs := map[string]*Transaction{}
	for tx := range txs {
		resultTxs[tx.TxID] = tx
	}

	if len(resultTxs) != 2 {
		t.Errorf("There were %d transactions in the channel, expected 2", len(resultTxs))
	}
}

func Test_SingleBlock_TwoTxs_SecondBlocked(t *testing.T) {
	txIDs := []string{"txID1", "txID2"}

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")}}
	rawBlock := createBlock(1,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", reads, writes),
			createTxBytes(txIDs[1], "chaincode", reads, writes)})

	analyzer := NewAnalyzer()
	txs, err := analyzer.Analyze(cached.WrapBlock(rawBlock))
	if err != nil {
		t.Error(err)
	}

	tx := <-txs
	if tx.TxID != txIDs[0] {
		t.Errorf("Wrong txID. Expected %v, got %v", txIDs[0], tx.TxID)
	}

	select {
	case tx, more := <-txs:
		if more {
			t.Errorf("There were 2 transactions in the channel, expected 1: %v", tx.TxID)
		}
	default:
	}
}

func Test_SingleBlock_TwoTxs_SecondFreedAfterFirstCommitted(t *testing.T) {
	txIDs := []string{"txID1", "txID2"}

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")}}
	rawBlock := createBlock(1,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", reads, writes),
			createTxBytes(txIDs[1], "chaincode", reads, writes)})

	analyzer := NewAnalyzer()
	txs, err := analyzer.Analyze(cached.WrapBlock(rawBlock))
	if err != nil {
		t.Error(err)
	}

	tx := <-txs
	if tx.TxID != txIDs[0] {
		t.Errorf("Wrong txID. Expected %v, got %v", txIDs[0], tx.TxID)
	}

	select {
	case tx = <-txs:
		t.Errorf("There were 2 transactions in the channel, expected 1: %v", tx.TxID)
	default:
	}

	analyzer.NotifyAboutCommit(tx)

	tx = <-txs
	if tx.TxID != txIDs[1] {
		t.Errorf("Wrong txID. Expected %v, got %v", txIDs[1], tx.TxID)
	}
}

func Test_TxWithoutKeys(t *testing.T) {
	txIDs := []string{"txID1", "txID2", "txID3"}

	rawBlock := createBlock(1,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", []*kvrwset.KVRead{}, []*kvrwset.KVWrite{}),
			createTxBytes(txIDs[1], "chaincode", []*kvrwset.KVRead{}, []*kvrwset.KVWrite{}),
			createTxBytes(txIDs[2], "chaincode", []*kvrwset.KVRead{}, []*kvrwset.KVWrite{})})

	analyzer := NewAnalyzer()
	txs, err := analyzer.Analyze(cached.WrapBlock(rawBlock))
	if err != nil {
		t.Error(err)
	}

	count := 0
	for range txs {
		count++
	}
	if count != 3 {
		t.Errorf("There were %d transactions in the channel, expected 3", count)
	}
}

func Test_MultipleBlocks_OutOfOrder(t *testing.T) {
	txIDs := []string{"txID1", "txID2", "txID3", "txID4", "txID5", "txID6"}

	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key3", Value: []byte("value2")},
		{Key: "key4", Value: []byte("value4")}}
	rawBlock1 := createBlock(1,
		[][]byte{
			createTxBytes(txIDs[0], "chaincode", reads, writes),
			createTxBytes(txIDs[1], "chaincode", reads, writes)})

	rawBlock2 := createBlock(2,
		[][]byte{
			createTxBytes(txIDs[2], "chaincode", reads, writes),
			createTxBytes(txIDs[3], "chaincode", reads, writes)})

	rawBlock3 := createBlock(3,
		[][]byte{
			createTxBytes(txIDs[4], "chaincode", reads, writes),
			createTxBytes(txIDs[5], "chaincode", reads, writes)})

	analyzer := NewAnalyzer()
	txs, err := analyzer.Analyze(cached.WrapBlock(rawBlock1))
	if err != nil {
		t.Error(err)
	}

	count := 0
	for tx := range txs {
		if tx.TxID != txIDs[count] {
			t.Errorf("Wrong txID. Expected %v, got %v", txIDs[count], tx.TxID)
		}
		analyzer.NotifyAboutCommit(tx)
		count++
	}
	if count != 2 {
		t.Errorf("There were %d transactions in the channel, expected 2", count)
	}

	txs3, err := analyzer.Analyze(cached.WrapBlock(rawBlock3))
	if err != nil {
		t.Error(err)
	}

	txs2, err := analyzer.Analyze(cached.WrapBlock(rawBlock2))
	if err != nil {
		t.Error(err)
	}

	closed2 := false
	closed3 := false
	for !closed2 || !closed3 {
		select {
		case tx, more := <-txs2:
			if more {
				if tx.TxID != txIDs[count] {
					t.Errorf("Wrong txID. Expected %v, got %v", txIDs[count], tx.TxID)
				}
				analyzer.NotifyAboutCommit(tx)
				count++
			} else {
				closed2 = true
			}
		case tx, more := <-txs3:
			if more {
				if tx.TxID != txIDs[count] {
					t.Errorf("Wrong txID. Expected %v, got %v", txIDs[count], tx.TxID)
				}
				analyzer.NotifyAboutCommit(tx)
				count++
			} else {
				closed3 = true
			}
		}
	}
	if count != 6 {
		t.Errorf("Expected 6 total txs, got %v", count)
	}
}

func Test_ManyBlocks_AllToSameKey(t *testing.T) {
	reads := []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 0}},
		{Key: "key2", Version: &kvrwset.Version{BlockNum: 1, TxNum: 10}}}
	writes := []*kvrwset.KVWrite{{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")}}

	blocks := []*common.Block{}

	for i := 0; i < 100; i++ {
		txs := [][]byte{}
		for j := 0; j < 100; j++ {
			txs = append(txs, createTxBytes("txID_"+strconv.Itoa(i)+"_"+strconv.Itoa(j), "chaincode", reads, writes))
		}
		blocks = append(blocks, createBlock(uint64(i),
			txs))
	}

	analyzer := NewAnalyzer()
	output := make(chan (<-chan *Transaction), len(blocks))
	go func() {
		for _, block := range blocks {
			o, err := analyzer.Analyze(cached.WrapBlock(block))
			if err != nil {
				t.Error(err)
			}
			output <- o
		}
		close(output)
	}()

	errChan := make(chan error, len(blocks))
	wg := &sync.WaitGroup{}
	wg.Add(len(blocks))
	for o := range output {
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			txCount := 0
			for tx := range o {
				analyzer.NotifyAboutCommit(tx)
				txCount++
			}
			if txCount != 100 {
				errChan <- fmt.Errorf("Got txCount: %d", txCount)
			}
		}(wg)
	}
	wg.Wait()
	select {
	case err := <-errChan:
		t.Error(err)
	default:
	}

}

func createBlock(blockNum uint64, txs [][]byte) *common.Block {
	return &common.Block{Header: &common.BlockHeader{Number: blockNum}, Data: &common.BlockData{Data: txs}}
}

func createTxBytes(txID string, chaincodeName string, reads []*kvrwset.KVRead, writes []*kvrwset.KVWrite) []byte {
	set := &cached.TxRwSet{
		NsRwSets: []*cached.NsRwSet{{
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
