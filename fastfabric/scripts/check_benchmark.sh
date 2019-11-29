#!/usr/bin/env bash
source base_parameters.sh

peer chaincode query -C ${CHANNEL} -n ${CHAINCODE} -c '{"Args":["query","'${1}'"]}'