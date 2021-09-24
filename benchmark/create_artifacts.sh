#!/usr/bin/bash
set -e
. config.sh

CHANNEL=mychannel

if [[ -d ./organizations ]]; then rm -r ./organizations; fi
if [[ -d ./channel-artifacts ]]; then rm -r ./channel-artifacts; fi

# mkdir peerOrganizations
mkdir channel-artifacts
./bin/cryptogen generate --config=crypto-config.yaml --output="organizations"
./bin/configtxgen -configPath ./ -outputBlock ./channel-artifacts/genesis.block -profile OneOrgOrdererGenesis -channelID ${CHANNEL}-system-channel
./bin/configtxgen -configPath ./ -outputCreateChannelTx ./channel-artifacts/channel.tx -profile OneOrgChannel -channelID ${CHANNEL}
./bin/configtxgen -configPath ./ -outputAnchorPeersUpdate ./channel-artifacts/anchor_peer.tx -profile OneOrgChannel -asOrg Org1MSP -channelID ${CHANNEL}

cp organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore/*sk organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore/priv_sk
tar -czvf data.tar.gz organizations channel-artifacts scripts chaincode tape.yaml core.yaml orderer.yaml > /dev/null

for host in ${hosts[@]}; do
    ssh $USER@$host "echo yunpeng | sudo -S rm -rf $workspace; mkdir $workspace"
    scp data.tar.gz $USER@$host:~/$workspace/ 
    ssh $USER@$host "cd $workspace; tar -xzvf data.tar.gz > /dev/null; rm data.tar.gz"
done
rm data.tar.gz



