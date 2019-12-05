/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statebasedval

import (
	"encoding/json"
	"errors"
	"github.com/go-python/gpython/repl"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/fastfabric/config"
	"github.com/hyperledger/fabric/fastfabric/dependency"
	"github.com/hyperledger/fabric/fastfabric/validator/internalVal"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"io/ioutil"
	"strings"
	"sync"

	// import required modules
	_ "github.com/go-python/gpython/builtin"
	_ "github.com/go-python/gpython/math"
	_ "github.com/go-python/gpython/sys"
	_ "github.com/go-python/gpython/time"
)

var logger = flogging.MustGetLogger("statebasedval")

// Validator validates a tx against the latest committed state
// and preceding valid transactions with in the same block
type Validator struct {
	db            privacyenabledstate.DB
	validatedKeys chan map[string]*version.Height
	vm            *repl.REPL
	output        func() string
	executeLock   sync.Mutex
}

// NewValidator constructs StateValidator
func NewValidator(db privacyenabledstate.DB) *Validator {
	ui := &mockUI{}
	validator := &Validator{
		db:            db,
		validatedKeys: make(chan map[string]*version.Height, 1),
		vm:            repl.New(),
		output:        func() string { return ui.output },
	}
	validator.validatedKeys <- make(map[string]*version.Height)
	validator.vm.SetUI(ui)

	data, err := ioutil.ReadFile("../chaincode/contract.py")
	if err != nil {
		logger.Fatal(err)
	}
	code := string(data)
	validator.vm.Run(code)

	return validator
}

// preLoadCommittedVersionOfRSet loads committed version of all keys in each
// transaction's read set into a cache.
func (v *Validator) preLoadCommittedVersionOfRSet(block *internalVal.Block) error {
	// Collect both public and hashed keys in read sets of all transactions in a given block
	var pubKeys []*statedb.CompositeKey
	var hashedKeys []*privacyenabledstate.HashedCompositeKey

	// pubKeysMap and hashedKeysMap are used to avoid duplicate entries in the
	// pubKeys and hashedKeys. Though map alone can be used to collect keys in
	// read sets and pass as an argument in LoadCommittedVersionOfPubAndHashedKeys(),
	// array is used for better code readability. On the negative side, this approach
	// might use some extra memory.
	pubKeysMap := make(map[statedb.CompositeKey]interface{})
	hashedKeysMap := make(map[privacyenabledstate.HashedCompositeKey]interface{})

	txs := block.Txs
	processedTxs := make(chan *dependency.Transaction, cap(txs))
	block.Txs = processedTxs
	for tx := range txs {
		for _, nsRWSet := range tx.RwSet.NsRwSets {
			for _, kvRead := range nsRWSet.KvRwSet.Reads {
				compositeKey := statedb.CompositeKey{
					Namespace: nsRWSet.NameSpace,
					Key:       kvRead.Key,
				}
				if _, ok := pubKeysMap[compositeKey]; !ok {
					pubKeysMap[compositeKey] = nil
					pubKeys = append(pubKeys, &compositeKey)
				}

			}
			for _, colHashedRwSet := range nsRWSet.CollHashedRwSets {
				for _, kvHashedRead := range colHashedRwSet.HashedRwSet.HashedReads {
					hashedCompositeKey := privacyenabledstate.HashedCompositeKey{
						Namespace:      nsRWSet.NameSpace,
						CollectionName: colHashedRwSet.CollectionName,
						KeyHash:        string(kvHashedRead.KeyHash),
					}
					if _, ok := hashedKeysMap[hashedCompositeKey]; !ok {
						hashedKeysMap[hashedCompositeKey] = nil
						hashedKeys = append(hashedKeys, &hashedCompositeKey)
					}
				}
			}
		}
		processedTxs <- tx
	}

	// Load committed version of all keys into a cache
	if len(pubKeys) > 0 || len(hashedKeys) > 0 {
		err := v.db.LoadCommittedVersionsOfPubAndHashedKeys(pubKeys, hashedKeys)
		if err != nil {
			return err
		}
	}

	return nil
}

// ValidateAndPrepareBatch implements method in Validator interface
func (v *Validator) ValidateAndPrepareBatch(block *internalVal.Block, doMVCCValidation bool, committedTxs chan<- *dependency.Transaction) (*internalVal.PubAndHashUpdates, error) {
	defer close(committedTxs)

	// Check whether statedb implements BulkOptimizable interface. For now,
	// only CouchDB implements BulkOptimizable to reduce the number of REST
	// API calls from peer to CouchDB instance.
	if v.db.IsBulkOptimizable() {
		err := v.preLoadCommittedVersionOfRSet(block)
		if err != nil {
			return nil, err
		}
	}

	updates := internalVal.NewPubAndHashUpdates()
	txs := block.Txs
	processedTxs := make(chan *dependency.Transaction, cap(txs))
	defer close(processedTxs)
	block.Txs = processedTxs

	for tx := range txs {
		var validationCode peer.TxValidationCode
		var err error
		if validationCode, err = v.validateEndorserTX(tx, doMVCCValidation, updates); err != nil {
			return nil, err
		}
		tx.ValidationCode = validationCode
		if validationCode == peer.TxValidationCode_VALID {
			logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator", block.Num, tx.Version.TxNum, tx.TxID)
			committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
			updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
		} else {
			logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
				block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
		}
		processedTxs <- tx
		committedTxs <- tx
	}
	return updates, nil
}

// validateEndorserTX validates endorser transaction
func (v *Validator) validateEndorserTX(
	tx *dependency.Transaction,
	doMVCCValidation bool,
	updates *internalVal.PubAndHashUpdates) (peer.TxValidationCode, error) {

	var validationCode = peer.TxValidationCode_VALID
	var err error
	//mvccvalidation, may invalidate transaction
	if doMVCCValidation {
		validationCode, err = v.validateTx(tx, updates)
	}
	if err != nil {
		return peer.TxValidationCode(-1), err
	}
	if validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT && tx.Version.BlockNum > 2 {
		validationCode = v.reExecute(tx, updates)
	}

	return validationCode, nil
}

func (v *Validator) validateTx(tx *dependency.Transaction, updates *internalVal.PubAndHashUpdates) (peer.TxValidationCode, error) {
	// Uncomment the following only for local debugging. Don't want to print data in the logs in production
	//logger.Debugf("validateTx - validating txRWSet: %s", spew.Sdump(txRWSet))

	if config.IsStorage || config.IsEndorser {
		return tx.ValidationCode, nil
	}

	for _, nsRWSet := range tx.RwSet.NsRwSets {
		ns := nsRWSet.NameSpace

		// Validate public reads
		if valid, err := v.validateReadSet(ns, nsRWSet.KvRwSet.Reads, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
		// Validate range queries for phantom items
		if valid, err := v.validateRangeQueries(ns, nsRWSet.KvRwSet.RangeQueriesInfo, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_PHANTOM_READ_CONFLICT, nil
		}
		// Validate hashes for private reads
		if valid, err := v.validateNsHashedReadSets(ns, nsRWSet.CollHashedRwSets, updates.HashUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
	}
	v.rememberWrites(tx.RwSet, tx.Version)

	return peer.TxValidationCode_VALID, nil
}

func (v *Validator) rememberWrites(rwset *cached.TxRwSet, txVersion *version.Height) {
	for _, set := range rwset.NsRwSets {
		validatedKeys := <-v.validatedKeys
		for _, write := range set.KvRwSet.Writes {

			validatedKeys[set.NameSpace+"_"+write.Key] = txVersion
		}
		v.validatedKeys <- validatedKeys
	}
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of public read-set
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateReadSet(ns string, kvReads []*kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	for _, kvRead := range kvReads {
		if valid, err := v.validateKVRead(ns, kvRead, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateKVRead performs mvcc check for a key read during transaction simulation.
// i.e., it checks whether a key/version combination is already updated in the statedb (by an already committed block)
// or in the updates (by a preceding valid transaction in the current block)
func (v *Validator) validateKVRead(ns string, kvRead *kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	if updates.Exists(ns, kvRead.Key) {
		return false, nil
	}

	committedVersion, err := v.db.GetVersion(ns, kvRead.Key)
	if err != nil {
		return false, err
	}

	logger.Debugf("Comparing versions for key [%s]: committed version=%#v and read version=%#v",
		kvRead.Key, committedVersion, rwsetutil.NewVersion(kvRead.Version))
	if !version.AreSame(committedVersion, rwsetutil.NewVersion(kvRead.Version)) {
		logger.Debugf("Version mismatch for key [%s:%s]. Committed version = [%#v], Version in readSet [%#v]",
			ns, kvRead.Key, committedVersion, kvRead.Version)
		return false, nil
	}

	validatedKeys := <-v.validatedKeys
	if ver, ok := validatedKeys[ns+"_"+kvRead.Key]; ok {
		if ver.Compare(committedVersion) <= 1 {
			delete(validatedKeys, ns+"_"+kvRead.Key)
			v.validatedKeys <- validatedKeys
			return true, nil
		}
		if !version.AreSame(ver, rwsetutil.NewVersion(kvRead.Version)) {
			v.validatedKeys <- validatedKeys
			return false, nil
		}
	}
	v.validatedKeys <- validatedKeys

	return true, nil
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of range queries
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateRangeQueries(ns string, rangeQueriesInfo []*kvrwset.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	for _, rqi := range rangeQueriesInfo {
		if valid, err := v.validateRangeQuery(ns, rqi, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateRangeQuery performs a phantom read check i.e., it
// checks whether the results of the range query are still the same when executed on the
// statedb (latest state as of last committed block) + updates (prepared by the writes of preceding valid transactions
// in the current block and yet to be committed as part of group commit at the end of the validation of the block)
func (v *Validator) validateRangeQuery(ns string, rangeQueryInfo *kvrwset.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	logger.Debugf("validateRangeQuery: ns=%s, rangeQueryInfo=%s", ns, rangeQueryInfo)

	// If during simulation, the caller had not exhausted the iterator so
	// rangeQueryInfo.EndKey is not actual endKey given by the caller in the range query
	// but rather it is the last key seen by the caller and hence the combinedItr should include the endKey in the results.
	includeEndKey := !rangeQueryInfo.ItrExhausted

	combinedItr, err := newCombinedIterator(v.db, updates.UpdateBatch,
		ns, rangeQueryInfo.StartKey, rangeQueryInfo.EndKey, includeEndKey)
	if err != nil {
		return false, err
	}
	defer combinedItr.Close()
	var validator rangeQueryValidator
	if rangeQueryInfo.GetReadsMerkleHashes() != nil {
		logger.Debug(`Hashing results are present in the range query info hence, initiating hashing based validation`)
		validator = &rangeQueryHashValidator{}
	} else {
		logger.Debug(`Hashing results are not present in the range query info hence, initiating raw KVReads based validation`)
		validator = &rangeQueryResultsValidator{}
	}
	validator.init(rangeQueryInfo, combinedItr)
	return validator.validate()
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of hashed read-set
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateNsHashedReadSets(ns string, collHashedRWSets []*cached.CollHashedRwSet,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, collHashedRWSet := range collHashedRWSets {
		if valid, err := v.validateCollHashedReadSet(ns, collHashedRWSet.CollectionName, collHashedRWSet.HashedRwSet.HashedReads, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateCollHashedReadSet(ns, coll string, kvReadHashes []*kvrwset.KVReadHash,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, kvReadHash := range kvReadHashes {
		if valid, err := v.validateKVReadHash(ns, coll, kvReadHash, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateKVReadHash performs mvcc check for a hash of a key that is present in the private data space
// i.e., it checks whether a key/version combination is already updated in the statedb (by an already committed block)
// or in the updates (by a preceding valid transaction in the current block)
func (v *Validator) validateKVReadHash(ns, coll string, kvReadHash *kvrwset.KVReadHash,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	if updates.Contains(ns, coll, kvReadHash.KeyHash) {
		return false, nil
	}
	committedVersion, err := v.db.GetKeyHashVersion(ns, coll, kvReadHash.KeyHash)
	if err != nil {
		return false, err
	}

	if !version.AreSame(committedVersion, rwsetutil.NewVersion(kvReadHash.Version)) {
		logger.Debugf("Version mismatch for key hash [%s:%s:%#v]. Committed version = [%s], Version in hashedReadSet [%s]",
			ns, coll, kvReadHash.KeyHash, committedVersion, kvReadHash.Version)
		return false, nil
	}
	return true, nil
}

type mockUI struct {
	output string
}

func (mockUI) SetPrompt(string) {
}

func (m *mockUI) Print(s string) {
	m.output = s
}

func (v *Validator) reExecute(tx *dependency.Transaction, updates *internalVal.PubAndHashUpdates) peer.TxValidationCode {
	v.executeLock.Lock()
	valCode := v.executeChaincode(tx, updates, v.db)
	v.executeLock.Unlock()
	if valCode == peer.TxValidationCode_VALID {
		v.rememberWrites(tx.RwSet, tx.Version)
	}
	return valCode
}

func (v *Validator) executeChaincode(transaction *dependency.Transaction, updates *internalVal.PubAndHashUpdates, db privacyenabledstate.DB) peer.TxValidationCode {
	rwset := prepareRwSet(transaction.RwSet, updates, db)
	benchmark := rwset[0]["benchmark"]
	delete(rwset[0], "benchmark")
	args := getArgs(transaction.Payload)

	pyRwSet := pythonifyRwSet(rwset)
	pyArgs := pythonifySlice(args)
	call := "execute(" + pyRwSet + "," + pyArgs + ")"
	v.vm.RunWithGas(call, config.Gas)
	if v.vm.OutOfGas {
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}

	newSet := toRwSetObject(v.output())
	newSet[0]["benchmark"] = benchmark
	if err := compareAndUpdate(transaction, newSet); err != nil {
		return peer.TxValidationCode_BAD_RWSET
	}
	return peer.TxValidationCode_VALID
}

func pythonifySlice(args [][]byte) string {
	sb := strings.Builder{}
	sb.WriteString("[")
	for i := 0; i < len(args); i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("\"")
		sb.Write(args[i])
		sb.WriteString("\"")
	}
	sb.WriteString("]")
	return sb.String()
}

func getArgs(payload *cached.Payload) [][]byte {
	tx, _ := payload.UnmarshalTransaction()
	cca, _ := tx.Actions[0].UnmarshalChaincodeActionPayload()
	ppl, _ := cca.UnmarshalProposalPayload()
	input, _ := ppl.UnmarshalInput()
	return input.ChaincodeSpec.Input.Args
}

func prepareRwSet(set *cached.TxRwSet, updates *internalVal.PubAndHashUpdates, db privacyenabledstate.DB) []map[string]interface{} {
	newSet := make([]map[string]interface{}, 3, 3)
	for i := 0; i < 3; i++ {
		newSet[i] = make(map[string]interface{})
	}
	for _, ns := range set.NsRwSets {
		for _, r := range ns.KvRwSet.Reads {
			val := updates.PubUpdates.Get(ns.NameSpace, r.Key)
			if val == nil {
				var err error
				val, err = db.GetState(ns.NameSpace, r.Key)
				if err != nil {
					panic(err)
				}
			}
			newSet[0][r.Key] = string(val.Value)
		}
		for _, w := range ns.KvRwSet.Writes {
			if !strings.HasPrefix(w.Key, "oracle_") {
				newSet[1][w.Key] = string(w.Value)
			} else {
				newSet[2][strings.TrimPrefix(w.Key, "oracle_")] = string(w.Value)
			}
		}
	}
	return newSet
}

func pythonifyRwSet(set []map[string]interface{}) string {
	s, _ := json.Marshal(set)
	return string(s)
}

func toRwSetObject(s string) []map[string]interface{} {
	var set []map[string]interface{}
	s = strings.Replace(s, "'", "\"", -1)
	json.Unmarshal([]byte(s), &set)
	return set
}

func compareAndUpdate(transaction *dependency.Transaction, newSet []map[string]interface{}) error {
	set := transaction.RwSet
	readCount := 0
	writeCount := 0
	oracleCount := 0
	hasError := false
	for _, ns := range set.NsRwSets {
		for _, r := range ns.KvRwSet.Reads {
			readCount++
			if _, ok := newSet[0][r.Key]; !ok {
				hasError = true
				break
			}

		}
		for _, w := range ns.KvRwSet.Writes {
			if !strings.HasPrefix(w.Key, "oracle_") {
				writeCount++
				if _, ok := newSet[1][w.Key]; !ok {
					hasError = true
					break
				} else {
					w.Value = []byte(newSet[1][w.Key].(string))
				}
			} else {
				oracleCount++
				if _, ok := newSet[2][strings.TrimPrefix(w.Key, "oracle_")]; !ok {
					hasError = true
					break
				}
			}
		}
	}
	if hasError {
		return errors.New("could not update transaction after re-execution")
	}
	return nil
}
