#!/bin/bash
source custom_parameters.sh
source base_parameters.sh
echo "==== Terminating storage peer ===="
if [[ -n ${STORAGE_ADDRESS} ]]; then
  ssh -T "$(get_correct_peer_address "${STORAGE_ADDRESS}")" '(killall peer)'
fi

if [[ -n ${VALIDATION_ADDRESS} ]]; then
  ssh -T "$(get_correct_peer_address "${VALIDATION_ADDRESS}")" '(killall preorderval)'
fi

echo "==== Terminating endorser ===="
for endorser in "${ENDORSER_ADDRESS[@]}"; do
  ssh -T "$(get_correct_peer_address "${endorser}")" '(killall peer)'
done

echo "==== Terminating fast peer ===="
ssh -T "$(get_correct_peer_address "${FAST_PEER_ADDRESS}")" '(killall peer)'

echo "==== Terminating orderer ===="
ssh -T "$(get_correct_orderer_address "${ORDERER_ADDRESS}")" '(killall orderer)'
