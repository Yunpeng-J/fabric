#!/usr/bin/env bash
source base_parameters.sh

export CORE_PEER_MSPCONFIGPATH=./crypto-config/peerOrganizations/${PEER_DOMAIN}/users/Admin@${PEER_DOMAIN}/msp
export CORE_PEER_LOCALMSPID=Org1MSP
CORE_PEER_ADDRESS="$(get_correct_peer_address "${FAST_PEER_ADDRESS}")":7051
export CORE_PEER_ADDRESS
peer chaincode query -C "${CHANNEL}" -n "${CHAINCODE}" -c '{"Args":["query","'"${1}"'"]}'