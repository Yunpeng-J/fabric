#!/bin/bash
source base_parameters.sh

export FABRIC_LOGGING_SPEC=INFO
export CORE_PEER_MSPCONFIGPATH=${FABRIC_CFG_PATH}/crypto-config/peerOrganizations/${PEER_DOMAIN}/peers/$p_addr/msp

p_addr="$(get_correct_peer_address "$(hostname)")"
export CORE_PEER_ID=${p_addr}
export CORE_PEER_ADDRESS=${p_addr}:7051
export CORE_PEER_GOSSIP_EXTERNALENDPOINT=${p_addr}:7051
export CORE_PEER_LISTENADDRESS=0.0.0.0:7051
export CORE_PEER_CHAINCODEADDRESS=${p_addr}:7052
export CORE_PEER_CHAINCODELISTENADDRESS=0.0.0.0:7052
export CORE_PEER_GOSSIP_USELEADERELECTION=false
export CORE_PEER_GOSSIP_ORGLEADER=false

export CORE_PEER_LOCALMSPID=Org1MSP

rm /var/hyperledger/production/* -r # clean up data from previous runs
(cd "${FABRIC_ROOT}" && make peer)

peer node start -s --storageAddr "${p_addr}":10000
