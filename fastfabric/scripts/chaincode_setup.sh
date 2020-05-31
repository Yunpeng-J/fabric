#!/bin/bash
source base_parameters.sh

endorsers=("${FAST_PEER_ADDRESS}")

if [[ ! ${#ENDORSER_ADDRESS[@]} -eq 0 ]]
then
    endorsers=("${endorsers[@]}" "${ENDORSER_ADDRESS[@]}")
fi

for endorser in "${endorsers[@]}"
do
    CORE_PEER_ADDRESS=$(get_correct_peer_address "${endorser}"):7051
    export CORE_PEER_ADDRESS
    peer chaincode install -l golang -n "${CHAINCODE}" -v 1.0 -o "$(get_correct_orderer_address)":7050 -p "github.com/hyperledger/fabric/fastfabric/chaincode"
done

CORE_PEER_ADDRESS=$(get_correct_peer_address "${endorsers[0]}"):7051
export CORE_PEER_ADDRESS
a="'{\"Args\":[\"init\",\"0\", \"1\", \"0\"]}'"
echo peer chaincode instantiate -o "$(get_correct_orderer_address)":7050 -C "${CHANNEL}" -n "${CHAINCODE}" -v 1.0 -c "${a}" | bash

sleep 5

bash chaincode_account_setup.sh "${1}" "${2}" "${3}"
