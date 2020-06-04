#!/usr/bin/env bash
source base_parameters.sh

(cd "${FABRIC_ROOT}" && make cryptogen)
(cd "${FABRIC_ROOT}" && make configtxgen)

if [[ ! -f core.yaml.bak ]]; then
  cp core.yaml core.yaml.bak
fi

bootstrap="bootstrap: $(get_correct_peer_address "${FAST_PEER_ADDRESS}"):7051"
sed <core.yaml.bak "s/bootstrap: 127.0.0.1:7051/${bootstrap}/g" >core.yaml

if [[ ! -f crypto-config.yaml.bak ]]; then
  cp crypto-config.yaml crypto-config.yaml.bak
fi

endorsers=""
for endorser in "${ENDORSER_ADDRESS[@]}"; do
  endorsers="- Hostname: ${endorser}\n    ${endorsers}"
done

storage=""
if [[ -n ${STORAGE_ADDRESS} ]]; then
  storage="- Hostname: ${STORAGE_ADDRESS}\n    ${storage}"
fi

sed <crypto-config.yaml.bak "s/ORDERER_DOMAIN/${ORDERER_DOMAIN}/g" | sed "s/ORDERER_ADDRESS/${ORDERER_ADDRESS}/g" | sed "s/PEER_DOMAIN/${PEER_DOMAIN}/g" | sed "s/FAST_PEER_ADDRESS/${FAST_PEER_ADDRESS}/g" | sed "s/ENDORSERS/${endorsers}/g" | sed "s/STORAGE/${storage}/g" >crypto-config.yaml

if [[ ! -f configtx.yaml.bak ]]; then
  cp configtx.yaml configtx.yaml.bak
fi

sed <configtx.yaml.bak "s/ORDERER_DOMAIN/${ORDERER_DOMAIN}/g" | sed "s/ORDERER_ADDRESS/${ORDERER_ADDRESS}/g" | sed "s/PEER_DOMAIN/${PEER_DOMAIN}/g" | sed "s/FAST_PEER_ADDRESS/${FAST_PEER_ADDRESS}/g" >configtx.yaml

if [[ -d ./crypto-config ]]; then rm -r ./crypto-config; fi
if [[ -d ./channel-artifacts ]]; then rm -r ./channel-artifacts; fi
mkdir "${FABRIC_CFG_PATH}"/channel-artifacts
cryptogen generate --config=crypto-config.yaml
configtxgen -configPath "${FABRIC_CFG_PATH}" -outputBlock "${FABRIC_CFG_PATH}"/channel-artifacts/genesis.block -profile OneOrgOrdererGenesis -channelID "${CHANNEL}"-system-channel
configtxgen -configPath "${FABRIC_CFG_PATH}" -outputCreateChannelTx "${FABRIC_CFG_PATH}"/channel-artifacts/channel.tx -profile OneOrgChannel -channelID "${CHANNEL}"
configtxgen -configPath "${FABRIC_CFG_PATH}" -outputAnchorPeersUpdate "${FABRIC_CFG_PATH}"/channel-artifacts/anchor_peer.tx -profile OneOrgChannel -asOrg Org1MSP -channelID "${CHANNEL}"
