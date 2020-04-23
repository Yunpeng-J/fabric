#!/usr/bin/env bash

export FABRIC_ROOT=${GOPATH}/src/github.com/hyperledger/fabric
export FABRIC_CFG_PATH=${FABRIC_ROOT}/fastfabric/scripts #change this if you want to copy the script folder somewhere else before modifying it

if ! echo "$PATH" | grep -q "${FABRIC_ROOT}"/.build/bin;then
    export PATH="${PATH}":"${FABRIC_ROOT}"/.build/bin
fi


# shellcheck source=./custom_parameters.sh
source "${FABRIC_CFG_PATH}"/custom_parameters.sh

get_correct_address () {
    if [[ ${1} != "localhost" ]]
    then
        echo "${1}"."${2}"
    else
        echo "${1}"
    fi
}

get_correct_peer_address(){
    get_correct_address "${1}" "${PEER_DOMAIN}"
}

get_correct_orderer_address(){
    get_correct_address "${ORDERER_ADDRESS}" "${ORDERER_DOMAIN}"
}

get_endorsers(){
    if [[ ${#ENDORSER_ADDRESS[@]} -eq 0 ]]; then
        echo "${FAST_PEER_ADDRESS}"
    else
        echo "${ENDORSER_ADDRESS[@]}"
    fi
}
