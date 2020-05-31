package validator

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	commonerrors "github.com/hyperledger/fabric/common/errors"
	"github.com/hyperledger/fabric/common/flogging"
	coreUtil "github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/chaincode/platforms"
	"github.com/hyperledger/fabric/core/chaincode/platforms/golang"
	"github.com/hyperledger/fabric/core/committer/txvalidator"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/common/sysccprovider"
	"github.com/hyperledger/fabric/core/common/validation"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/protos/common"
	msp2 "github.com/hyperledger/fabric/protos/msp"
	mspprotos "github.com/hyperledger/fabric/protos/msp"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/hyperledger/fabric/common/channelconfig"
	"net"
)

var ValidatorAddress string
var logger = flogging.MustGetLogger("preorder.validator")

type server struct {
	MockVal         bool
	sysCCs          []*SysCC
	ccDefs          map[string]*ccprovider.ChaincodeData
	policyEvaluator txvalidator.PolicyEvaluator
	mspCfgHandler   *channelconfig.MSPConfigHandler
}

func (s *server) Validate(_ context.Context, env *common.Envelope) (*ValidationResult, error) {
	return &ValidationResult{Code: s.DoValidate(&cached.Envelope{Envelope: env})}, nil
}

var chains = make(map[string]bool)

func (s *server) SetSysCC(_ context.Context, sys *SysCC) (*Result, error) {
	s.sysCCs = append(s.sysCCs, sys)
	return &Result{}, nil
}

func (s *server) ProposeMSP(_ context.Context, mspConfig *mspprotos.MSPConfig) (*Result, error) {
	_, err := s.mspCfgHandler.ProposeMSP(mspConfig)
	return &Result{}, err
}

func (s *server) SetChain(_ context.Context, chain *Chain) (*Result, error) {
	chains[chain.Name] = true
	manager, err := s.mspCfgHandler.CreateMSPManager()
	if err != nil {
		return nil, err
	}
	mgmt.XXXSetMSPManager(chain.Name, manager)
	return &Result{}, nil
}

func (s *server) SetCCDefs(_ context.Context, data *CCDef) (*Result, error) {
	var def = &ccprovider.ChaincodeData{}
	if err := proto.Unmarshal(data.Data, def); err != nil {
		return nil, err
	}

	s.ccDefs[def.Name] = def
	return &Result{}, nil
}

func StartServer(address string, isMock bool) {
	ValidatorAddress = address
	newServer := &server{
		ccDefs:          make(map[string]*ccprovider.ChaincodeData),
		policyEvaluator: txvalidator.PolicyEvaluator{},
		MockVal:         isMock,
		mspCfgHandler:   channelconfig.NewMSPConfigHandler(msp.MSPv1_4_3),
	}

	lis, err := net.Listen("tcp", address+":11000")
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	RegisterPreordervalidatorServer(s, newServer)
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}

func (s *server) DoValidate(env *cached.Envelope) peer.TxValidationCode {
	// validate the transaction: here we check that the transaction
	// is properly formed, properly signed and that the security
	// chain binding proposal to endorsements to tx holds. We do
	// NOT check the validity of endorsements, though. That's a
	// job for VSCC below
	var payload *cached.Payload
	var err error
	var txResult peer.TxValidationCode

	if s.MockVal {
		return peer.TxValidationCode_VALID
	}

	if env == nil {
		return peer.TxValidationCode_NIL_ENVELOPE
	}

	if payload, txResult = validation.ValidateTransaction(env, nil /* parameter isn't used*/); txResult != peer.TxValidationCode_VALID {
		return txResult
	}

	chdr, err := payload.Header.UnmarshalChannelHeader()
	if err != nil {
		logger.Warningf("Could not unmarshal channel header, err %s, skipping", err)
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}

	channel := chdr.ChannelId
	logger.Debugf("Transaction is for channel %s", channel)

	if !chainExists(channel) {
		logger.Errorf("Dropping transaction for non-existent channel %s", channel)
		return peer.TxValidationCode_TARGET_CHAIN_NOT_FOUND
	}

	if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {

		txID := chdr.TxId

		// Validate tx with vscc and policy
		logger.Debug("Validating transaction vscc tx validate")
		err, cde := s.VSCCValidateTx(payload)
		if err != nil {
			logger.Errorf("VSCCValidateTx for transaction txId = %s returned error: %s", txID, err)
			switch err.(type) {
			case *commonerrors.VSCCExecutionFailureError:
				return peer.TxValidationCode_INVALID_OTHER_REASON
			case *commonerrors.VSCCInfoLookupFailureError:
				return peer.TxValidationCode_INVALID_OTHER_REASON
			default:
				return cde
			}
		}

		_, _, err = getTxCCInstance(payload)
		if err != nil {
			return peer.TxValidationCode_INVALID_OTHER_REASON
		}
	}

	return peer.TxValidationCode_VALID
}

func getTxCCInstance(payload *cached.Payload) (invokeCCIns, upgradeCCIns *sysccprovider.ChaincodeInstance, err error) {
	// This is duplicated unpacking work, but make test easier.
	chdr, err := payload.Header.UnmarshalChannelHeader()
	if err != nil {
		return nil, nil, err
	}

	// Chain ID
	chainID := chdr.ChannelId // it is guaranteed to be an existing channel by now

	// ChaincodeID
	hdrExt, err := payload.Header.UnmarshalChaincodeHeaderExtension()
	if err != nil {
		return nil, nil, err
	}
	invokeCC := hdrExt.ChaincodeId
	invokeIns := &sysccprovider.ChaincodeInstance{ChainID: chainID, ChaincodeName: invokeCC.Name, ChaincodeVersion: invokeCC.Version}

	// Transaction
	tx, err := payload.UnmarshalTransaction()
	if err != nil {
		logger.Errorf("GetTransaction failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeActionPayload
	cap, err := tx.UnmarshalChaincodeActionPayload()
	if err != nil {
		logger.Errorf("GetChaincodeActionPayload failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeProposalPayload
	cpp, err := cap.UnmarshalProposalPayload()
	if err != nil {
		logger.Errorf("GetChaincodeProposalPayload failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeInvocationSpec
	cis, err := cpp.UnmarshalInput()
	if err != nil {
		logger.Errorf("GetChaincodeInvokeSpec failed: %+v", err)
		return invokeIns, nil, nil
	}

	if invokeCC.Name == "lscc" {
		if string(cis.ChaincodeSpec.Input.Args[0]) == "upgrade" {
			upgradeIns, err := getUpgradeTxInstance(chainID, cis.ChaincodeSpec.Input.Args[2])
			if err != nil {
				return invokeIns, nil, nil
			}
			return invokeIns, upgradeIns, nil
		}
	}

	return invokeIns, nil, nil
}

func getUpgradeTxInstance(chainID string, cdsBytes []byte) (*sysccprovider.ChaincodeInstance, error) {
	cds, err := utils.GetChaincodeDeploymentSpec(cdsBytes, platforms.NewRegistry(&golang.Platform{}))
	if err != nil {
		return nil, err
	}

	return &sysccprovider.ChaincodeInstance{
		ChainID:          chainID,
		ChaincodeName:    cds.ChaincodeSpec.ChaincodeId.Name,
		ChaincodeVersion: cds.ChaincodeSpec.ChaincodeId.Version,
	}, nil
}

func chainExists(channel string) bool {
	_, ok := chains[channel]
	return ok
}

func (s *server) VSCCValidateTx(payload *cached.Payload) (error, peer.TxValidationCode) {

	// get header extensions so we have the chaincode ID
	hdrExt, err := payload.Header.UnmarshalChaincodeHeaderExtension()
	if err != nil {
		return err, peer.TxValidationCode_BAD_HEADER_EXTENSION
	}

	// get channel header
	chdr, err := payload.Header.UnmarshalChannelHeader()
	if err != nil {
		return err, peer.TxValidationCode_BAD_CHANNEL_HEADER
	}

	/* obtain the list of namespaces we're writing stuff to;
	   at first, we establish a few facts about this invocation:
	   1) which namespaces does it write to?
	   2) does it write to LSCC's namespace?
	   3) does it write to any cc that cannot be invoked? */
	writesToLSCC := false
	writesToNonInvokableSCC := false
	cca, err := payload.UnmarshalChaincodeAction()
	if err != nil {
		return errors.WithMessage(err, "GetActionFromEnvelope failed"), peer.TxValidationCode_BAD_RESPONSE_PAYLOAD
	}
	txRWSet := &cached.TxRwSet{}
	if txRWSet, err = cca.UnmarshalRwSet(); err != nil {
		return errors.WithMessage(err, "txRWSet.FromProtoBytes failed"), peer.TxValidationCode_BAD_RWSET
	}

	// Verify the header extension and response payload contain the ChaincodeId
	if hdrExt.ChaincodeId == nil {
		return errors.New("nil ChaincodeId in header extension"), peer.TxValidationCode_INVALID_OTHER_REASON
	}

	if cca.ChaincodeId == nil {
		return errors.New("nil ChaincodeId in ChaincodeAction"), peer.TxValidationCode_INVALID_OTHER_REASON
	}

	// get name and version of the cc we invoked
	ccID := hdrExt.ChaincodeId.Name
	ccVer := cca.ChaincodeId.Version

	// sanity check on ccID
	if ccID == "" {
		err = errors.New("invalid chaincode ID")
		logger.Errorf("%+v", err)
		return err, peer.TxValidationCode_INVALID_OTHER_REASON
	}
	if ccID != cca.ChaincodeId.Name {
		err = errors.Errorf("inconsistent ccid info (%s/%s)", ccID, cca.ChaincodeId.Name)
		logger.Errorf("%+v", err)
		return err, peer.TxValidationCode_INVALID_OTHER_REASON
	}
	// sanity check on ccver
	if ccVer == "" {
		err = errors.New("invalid chaincode version")
		logger.Errorf("%+v", err)
		return err, peer.TxValidationCode_INVALID_OTHER_REASON
	}

	var wrNamespace []string
	alwaysEnforceOriginalNamespace := true // =>V1_2Validation
	if alwaysEnforceOriginalNamespace {
		wrNamespace = append(wrNamespace, ccID)
		if cca.Events != nil {
			ccEvent := &peer.ChaincodeEvent{}
			if ccEvent, err = cca.UnmarshalEvents(); err != nil {
				return errors.Wrapf(err, "invalid chaincode event"), peer.TxValidationCode_INVALID_OTHER_REASON
			}
			if ccEvent.ChaincodeId != ccID {
				return errors.Errorf("chaincode event chaincode id does not match chaincode action chaincode id"), peer.TxValidationCode_INVALID_OTHER_REASON
			}
		}
	}

	for _, ns := range txRWSet.NsRwSets {
		if !txWritesToNamespace(ns) {
			continue
		}

		// Check to make sure we did not already populate this chaincode
		// name to avoid checking the same namespace twice
		if ns.NameSpace != ccID || !alwaysEnforceOriginalNamespace {
			wrNamespace = append(wrNamespace, ns.NameSpace)
		}

		if !writesToLSCC && ns.NameSpace == "lscc" {
			writesToLSCC = true
		}

		if !writesToNonInvokableSCC && s.IsSysCCAndNotInvokableCC2CC(ns.NameSpace) {
			writesToNonInvokableSCC = true
		}

		if !writesToNonInvokableSCC && s.IsSysCCAndNotInvokableExternal(ns.NameSpace) {
			writesToNonInvokableSCC = true
		}
	}

	// we've gathered all the info required to proceed to validation;
	// validation will behave differently depending on the type of
	// chaincode (system vs. application)

	if !s.IsSysCC(ccID) {
		// if we're here, we know this is an invocation of an application chaincode;
		// first of all, we make sure that:
		// 1) we don't write to LSCC - an application chaincode is free to invoke LSCC
		//    for instance to get information about itself or another chaincode; however
		//    these legitimate invocations only ready from LSCC's namespace; currently
		//    only two functions of LSCC write to its namespace: deploy and upgrade and
		//    neither should be used by an application chaincode
		if writesToLSCC {
			return errors.Errorf("chaincode %s attempted to write to the namespace of LSCC", ccID),
				peer.TxValidationCode_ILLEGAL_WRITESET
		}
		// 2) we don't write to the namespace of a chaincode that we cannot invoke - if
		//    the chaincode cannot be invoked in the first place, there's no legitimate
		//    way in which a transaction has a write set that writes to it; additionally
		//    we don't have any means of verifying whether the transaction had the rights
		//    to perform that write operation because in v1, system chaincodes do not have
		//    any endorsement policies to speak of. So if the chaincode can't be invoked
		//    it can't be written to by an invocation of an application chaincode
		if writesToNonInvokableSCC {
			return errors.Errorf("chaincode %s attempted to write to the namespace of a system chaincode that cannot be invoked", ccID),
				peer.TxValidationCode_ILLEGAL_WRITESET
		}

		// validate *EACH* read write set according to its chaincode's endorsement policy
		for _, ns := range wrNamespace {
			// Get latest chaincode version, vscc and validate policy
			txcc, _, policy, err := s.GetInfoForValidate(chdr, ns)
			if err != nil {
				logger.Errorf("GetInfoForValidate for txId = %s returned error: %+v", chdr.TxId, err)
				return err, peer.TxValidationCode_INVALID_OTHER_REASON
			}

			// if the namespace corresponds to the cc that was originally
			// invoked, we check that the version of the cc that was
			// invoked corresponds to the version that lscc has returned
			if ns == ccID && txcc.ChaincodeVersion != ccVer {
				err = errors.Errorf("chaincode %s:%s/%s didn't match %s:%s/%s in lscc", ccID, ccVer, chdr.ChannelId, txcc.ChaincodeName, txcc.ChaincodeVersion, chdr.ChannelId)
				logger.Errorf("%+v", err)
				return err, peer.TxValidationCode_EXPIRED_CHAINCODE
			}

			if err = s.VSCCValidateTxForCC(payload, policy); err != nil {
				switch err.(type) {
				case *commonerrors.VSCCEndorsementPolicyError:
					return err, peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE
				default:
					return err, peer.TxValidationCode_INVALID_OTHER_REASON
				}
			}
		}
	}
	return nil, peer.TxValidationCode_VALID
}

func (s *server) VSCCValidateTxForCC(pl *cached.Payload, policy []byte) error {
	err := s.ValidateVSCC(pl, policy)
	if err == nil {
		return nil
	}

	// Else, treat it as an endorsement error.
	return &commonerrors.VSCCEndorsementPolicyError{Err: err}
}

func txWritesToNamespace(ns *cached.NsRwSet) bool {
	// check for public writes first
	if ns.KvRwSet != nil && len(ns.KvRwSet.Writes) > 0 {
		return true
	}

	// only look at collection data if we support that capability
	if false { // PrivateChannelData
		// check for private writes for all collections
		for _, c := range ns.CollHashedRwSets {
			if c.HashedRwSet != nil && len(c.HashedRwSet.HashedWrites) > 0 {
				return true
			}

			// only look at private metadata writes if we support that capability
			if false { //KeyLevelEndorsement
				// private metadata updates
				if c.HashedRwSet != nil && len(c.HashedRwSet.MetadataWrites) > 0 {
					return true
				}
			}
		}
	}

	// only look at metadata writes if we support that capability
	if false { //KeyLevelEndorsement
		// public metadata updates
		if ns.KvRwSet != nil && len(ns.KvRwSet.MetadataWrites) > 0 {
			return true
		}
	}

	return false
}

func (s *server) GetInfoForValidate(chdr *cached.ChannelHeader, ccID string) (*sysccprovider.ChaincodeInstance, *sysccprovider.ChaincodeInstance, []byte, error) {
	cc := &sysccprovider.ChaincodeInstance{
		ChainID:          chdr.ChannelId,
		ChaincodeName:    ccID,
		ChaincodeVersion: coreUtil.GetSysCCVersion(),
	}
	vscc := &sysccprovider.ChaincodeInstance{
		ChainID:          chdr.ChannelId,
		ChaincodeName:    "vscc",                     // default vscc for system chaincodes
		ChaincodeVersion: coreUtil.GetSysCCVersion(), // Get vscc version
	}
	var policy []byte
	if !s.IsSysCC(ccID) {
		// when we are validating a chaincode that is not a
		// system CC, we need to ask the CC to give us the name
		// of VSCC and of the policy that should be used

		// obtain name of the VSCC and the policy
		cd, err := s.getCDataForCC(ccID)
		if err != nil {
			msg := fmt.Sprintf("Unable to get chaincode data from ledger for txid %s, due to %s", chdr.TxId, err)
			logger.Errorf(msg)
			return nil, nil, nil, err
		}
		cc.ChaincodeName = cd.CCName()
		cc.ChaincodeVersion = cd.CCVersion()
		vscc.ChaincodeName, policy = cd.Validation()
	}

	return cc, vscc, policy, nil
}

// IsSysCC returns true if the supplied chaincode is a system chaincode
func (s *server) IsSysCC(name string) bool {
	for _, sysCC := range s.sysCCs {
		if sysCC.Name == name {
			return true
		}
	}
	if isDeprecatedSysCC(name) {
		return true
	}
	return false
}

// IsSysCCAndNotInvokableCC2CC returns true if the chaincode
// is a system chaincode and *CANNOT* be invoked through
// a cc2cc invocation
func (s *server) IsSysCCAndNotInvokableCC2CC(name string) bool {
	for _, sysCC := range s.sysCCs {
		if sysCC.Name == name {
			return !sysCC.InvokableCC2CC
		}
	}

	if isDeprecatedSysCC(name) {
		return true
	}

	return false
}

// IsSysCCAndNotInvokableExternal returns true if the chaincode
// is a system chaincode and *CANNOT* be invoked through
// a proposal to this peer
func (s *server) IsSysCCAndNotInvokableExternal(name string) bool {
	for _, sysCC := range s.sysCCs {
		if sysCC.Name == name {
			return !sysCC.InvokableExternal
		}
	}

	if isDeprecatedSysCC(name) {
		return true
	}

	return false
}

func isDeprecatedSysCC(name string) bool {
	return name == "vscc" || name == "escc"
}

func (s *server) getCDataForCC(ccid string) (ccprovider.ChaincodeDefinition, error) {
	cd, ok := s.ccDefs[ccid]
	if !ok {
		return nil, errors.New("chaincode not found")
	}

	if cd.Vscc == "" {
		return nil, errors.Errorf("lscc's state for [%s] is invalid, vscc field must be set", ccid)
	}

	if len(cd.Policy) == 0 {
		return nil, errors.Errorf("lscc's state for [%s] is invalid, policy field must be set", ccid)
	}

	return cd, nil
}

// Validate validates the given envelope corresponding to a transaction with an endorsement
// policy as given in its serialized form
func (s *server) ValidateVSCC(
	payl *cached.Payload,
	policyBytes []byte,
) commonerrors.TxValidationError {

	chdr, err := payl.Header.UnmarshalChannelHeader()
	if err != nil {
		return policyErr(err)
	}

	// validate the payload type
	if common.HeaderType(chdr.Type) != common.HeaderType_ENDORSER_TRANSACTION {
		logger.Errorf("Only Endorser Transactions are supported, provided type %d", chdr.Type)
		return policyErr(fmt.Errorf("Only Endorser Transactions are supported, provided type %d", chdr.Type))
	}

	// ...and the transaction...
	tx, err := payl.UnmarshalTransaction()
	if err != nil {
		logger.Errorf("VSCC error: GetTransaction failed, err %s", err)
		return policyErr(err)
	}

	cap, err := tx.UnmarshalChaincodeActionPayload()
	if err != nil {
		logger.Errorf("VSCC error: GetChaincodeActionPayload failed, err %s", err)
		return policyErr(err)
	}

	signatureSet, err := deduplicateIdentity(cap)
	if err != nil {
		return policyErr(err)
	}

	// evaluate the signature set against the policy
	err = s.policyEvaluator.Evaluate(policyBytes, signatureSet)
	if err != nil {
		logger.Warningf("Endorsement policy failure for transaction txid=%s, err: %s", chdr.GetTxId(), err.Error())
		if len(signatureSet) < len(cap.Action.Endorsements) {
			// Warning: duplicated identities exist, endorsement failure might be cause by this reason
			return policyErr(errors.New(DUPLICATED_IDENTITY_ERROR))
		}
		return policyErr(fmt.Errorf("VSCC error: endorsement policy failure, err: %s", err))
	}

	return nil
}

func policyErr(err error) *commonerrors.VSCCEndorsementPolicyError {
	return &commonerrors.VSCCEndorsementPolicyError{
		Err: err,
	}
}

const (
	DUPLICATED_IDENTITY_ERROR = "Endorsement policy evaluation failure might be caused by duplicated identities"
)

func deduplicateIdentity(cap *cached.ChaincodeActionPayload) ([]*common.SignedData, error) {
	// this is the first part of the signed message
	prespBytes := cap.Action.ProposalResponsePayload

	// build the signature set for the evaluation
	signatureSet := []*common.SignedData{}
	signatureMap := make(map[string]struct{})
	// loop through each of the endorsements and build the signature set
	for _, endorsement := range cap.Action.Endorsements {
		//unmarshal endorser bytes
		serializedIdentity := &msp2.SerializedIdentity{}
		if err := proto.Unmarshal(endorsement.Endorser, serializedIdentity); err != nil {
			logger.Errorf("Unmarshal endorser error: %s", err)
			return nil, policyErr(fmt.Errorf("Unmarshal endorser error: %s", err))
		}
		identity := serializedIdentity.Mspid + string(serializedIdentity.IdBytes)
		if _, ok := signatureMap[identity]; ok {
			// Endorsement with the same identity has already been added
			logger.Warningf("Ignoring duplicated identity, Mspid: %s, pem:\n%s", serializedIdentity.Mspid, serializedIdentity.IdBytes)
			continue
		}
		data := make([]byte, len(prespBytes)+len(endorsement.Endorser))
		copy(data, prespBytes)
		copy(data[len(prespBytes):], endorsement.Endorser)
		signatureSet = append(signatureSet, &common.SignedData{
			// set the data that is signed; concatenation of proposal response bytes and endorser ID
			Data: data,
			// set the identity that signs the message: it's the endorser
			Identity: endorsement.Endorser,
			// set the signature
			Signature: endorsement.Signature})
		signatureMap[identity] = struct{}{}
	}

	logger.Debugf("Signature set is of size %d out of %d endorsement(s)", len(signatureSet), len(cap.Action.Endorsements))
	return signatureSet, nil
}

func StartValidatorClient(address string) (PreordervalidatorClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return NewPreordervalidatorClient(conn), nil
}
