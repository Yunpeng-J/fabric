#!/bin/bash
source base_parameters.sh

(cd ${FABRIC_ROOT}/fastfabric/preorderval/ && go install)
preorderval $(hostname)