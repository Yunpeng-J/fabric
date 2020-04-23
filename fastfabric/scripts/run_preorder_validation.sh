#!/bin/bash
source base_parameters.sh

(cd "${FABRIC_ROOT}"&& make preorderval)
preorderval "$(hostname)"