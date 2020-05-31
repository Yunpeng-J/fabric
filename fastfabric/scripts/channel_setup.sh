#!/bin/bash
source base_parameters.sh

export CORE_PEER_MSPCONFIGPATH="${FABRIC_CFG_PATH}"/crypto-config/peerOrganizations/"${PEER_DOMAIN}"/users/Admin@"${PEER_DOMAIN}"/msp
export CORE_PEER_LOCALMSPID=Org1MSP

CORE_PEER_ADDRESS="$(get_correct_peer_address "${FAST_PEER_ADDRESS}")":7051
export CORE_PEER_ADDRESS

peer channel create -o "$(get_correct_orderer_address)":7050 -c "${CHANNEL}" -f "${FABRIC_CFG_PATH}"/channel-artifacts/channel.tx

if [[ -n $STORAGE_ADDRESS ]];then
    CORE_PEER_ADDRESS="$(get_correct_peer_address "${STORAGE_ADDRESS}")":7051
    export CORE_PEER_ADDRESS
    peer channel join -b "${CHANNEL}".block
fi

for endorser in "${ENDORSER_ADDRESS[@]}"
do
    CORE_PEER_ADDRESS="$(get_correct_peer_address "${endorser}")":7051
    export CORE_PEER_ADDRESS
    peer channel join -b "${CHANNEL}".block
done

CORE_PEER_ADDRESS="$(get_correct_peer_address "${FAST_PEER_ADDRESS}")":7051
export CORE_PEER_ADDRESS
peer channel join -b "${CHANNEL}".block
peer channel update -o "$(get_correct_orderer_address)":7050 -c "${CHANNEL}" -f "${FABRIC_CFG_PATH}"/channel-artifacts/anchor_peer.tx
