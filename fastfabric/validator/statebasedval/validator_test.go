package statebasedval

import (
	"fmt"
	"github.com/go-python/gpython/repl"
	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/mock"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/fastfabric/dependency"
	"github.com/hyperledger/fabric/fastfabric/validator/internalVal"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"testing"

	// import required modules
	_ "github.com/go-python/gpython/builtin"
	_ "github.com/go-python/gpython/math"
	_ "github.com/go-python/gpython/sys"
	_ "github.com/go-python/gpython/time"
)

func TestReExecution(t *testing.T) {
	mockDB := &mock.VersionedDB{}
	mockDB.GetStateStub = getstate
	db, _ := privacyenabledstate.NewCommonStorageDB(mockDB, "", nil)

	codePath = "../../chaincode/contract.py"
	v := NewValidator(db)

	pb, _ := proto.Marshal(&peer.ChaincodeInvocationSpec{ChaincodeSpec: &peer.ChaincodeSpec{Input: &peer.ChaincodeInput{Args: [][]byte{[]byte("account0"), []byte("account1"), []byte("20")}}}})
	pb, _ = proto.Marshal(&peer.ChaincodeProposalPayload{Input: pb})
	pb, _ = proto.Marshal(&peer.ChaincodeActionPayload{ChaincodeProposalPayload: pb})
	pb, _ = proto.Marshal(&peer.Transaction{Actions: []*peer.TransactionAction{{Payload: pb}}})
	pl := &cached.Payload{
		Payload: &common.Payload{Data: pb},
		Header:  nil,
	}
	val := v.executeChaincode(&dependency.Transaction{
		Payload: pl,
		RwSet: &cached.TxRwSet{
			NsRwSets: []*cached.NsRwSet{{NameSpace: "benchmark",
				KvRwSet: &kvrwset.KVRWSet{Reads: []*kvrwset.KVRead{{Key: "account0", Version: &kvrwset.Version{BlockNum: 1, TxNum: 1}},
					{Key: "account1", Version: &kvrwset.Version{BlockNum: 1, TxNum: 1}}},
					Writes: []*kvrwset.KVWrite{{Key: "account0", Value: []byte("80")}, {Key: "account1", Value: []byte("120")}}}}}}}, internalVal.NewPubAndHashUpdates(), v.db)

	if val != 0 {
		t.Errorf("Expected validation code to be %d but got %d", peer.TxValidationCode_VALID, val)
	}

}

func getstate(ns string, key string) (*statedb.VersionedValue, error) {
	if key == "account0" || key == "account1" {
		return &statedb.VersionedValue{Value: []byte("100")}, nil
	}
	return nil, fmt.Errorf("Not found")
}

func TestRepl(t *testing.T) {
	code := `def execute(rwset, args):
	account1 = args[0]
	account2 = args[1]
	transfer_amount = int(args[2])
	acc1_value = int(rwset[0][account1])
	acc2_value = int(rwset[0][account2])
	if acc1_value - transfer_amount >= 0:
	rwset[1][account1] = str(acc1_value - transfer_amount)
	rwset[1][account2] = str(acc2_value + transfer_amount)

	return rwset`

	r := repl.New()
	r.SetUI(&mockUI{})
	r.Run(code)
}

type replTest struct {
	prompt string
	out    string
}

// SetPrompt sets the current terminal prompt
func (rt *replTest) SetPrompt(prompt string) {
	rt.prompt = prompt
}

// Print prints the output
func (rt *replTest) Print(out string) {
	rt.out = out
}

func (rt *replTest) assert(t *testing.T, what, wantPrompt, wantOut string) {
	if rt.prompt != wantPrompt {
		t.Errorf("%s: Prompt wrong, want %q got %q", what, wantPrompt, rt.prompt)
	}
	if rt.out != wantOut {
		t.Errorf("%s: Output wrong, want %q got %q", what, wantOut, rt.out)
	}
	rt.out = ""
}

func TestREPL(t *testing.T) {
	r := repl.New()
	rt := &replTest{}
	r.SetUI(rt)

	rt.assert(t, "init", repl.NormalPrompt, "")

	r.RunWithGas("", 1)
	rt.assert(t, "empty", repl.NormalPrompt, "")

	r.RunWithGas("1+2", 7)
	rt.assert(t, "1+2", repl.NormalPrompt, "3")

	// FIXME this output goes to Stderr and Stdout
	r.RunWithGas("aksfjakf", 2)
	rt.assert(t, "unbound", repl.NormalPrompt, "")

	r.RunWithGas("sum = 0", 5)

	rt.assert(t, "multi#1", repl.NormalPrompt, "")
	r.RunWithGas("for i in range(10):", 1)
	rt.assert(t, "multi#2", repl.ContinuationPrompt, "")
	r.RunWithGas("    sum += i", 1)
	rt.assert(t, "multi#3", repl.ContinuationPrompt, "")
	r.RunWithGas("", 80)
	rt.assert(t, "multi#4", repl.NormalPrompt, "")
	r.RunWithGas("sum", 5)
	rt.assert(t, "multi#5", repl.NormalPrompt, "45")

	r.RunWithGas("if", 2)
	rt.assert(t, "compileError", repl.NormalPrompt, "Compile error: \n  File \"<string>\", line 1, offset 2\n    if\n\n\nSyntaxError: 'invalid syntax'")

	r.RunWithGas("2+1", 1)
	if !r.OutOfGas {
		t.Error("expected to run out of gas")
	}
	r.Run("2+1")
	if r.OutOfGas {
		t.Error("did not expect to run out of gas")
	}
}
