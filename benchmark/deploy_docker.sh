#!/bin/bash

if [ $# -ne 3 ]; then
    echo "three parameters are needed for deploy_docker.sh"
    echo "bash deploy.sh system_name old_tag new_tag"
    exit 1
fi

system=$1
old=$2
new=$3

if [ $system != "fabric" ] && [ $system != "fabric-sharp" ]; then 
    echo "unknown system: $system"
    echo "only support fabric, fabric-sharp"
    exit 1
fi

. config.sh 
echo "system:$system old_tag:$old new_tag:$new"


set -x
docker tag hyperledger/${system}-peer:$old hyperledger/${system}-peer:$new
docker tag hyperledger/${system}-orderer:$old hyperledger/${system}-orderer:$new
docker tag hyperledger/${system}-tools:$old hyperledger/${system}-tools:$new

docker save -o hlf_${system}_${new}_peer.tar hyperledger/${system}-peer:$new
docker save -o hlf_${system}_${new}_orderer.tar hyperledger/${system}-orderer:$new
# docker save -o hlf_tools.tar hyperledger/fabric-tools:$tag
set +x

for host in ${hosts[@]}; do
    echo "###  $host  ###"
    ifconfig | grep $host > /dev/null 
    if [ $? == 0 ]; then 
        continue
    fi
    scp hlf_${system}_${new}_peer.tar $USER@$host:~/
    scp hlf_${system}_${new}_orderer.tar $USER@$host:~/
    # scp hlf_tools.tar $USER@$host:~/
done

# deploy to all servers
for host in ${hosts[@]}; do
    ifconfig | grep $host > /dev/null 
    if [ $? == 0 ]; then 
        continue
    fi
    echo "###  $host  ###"
    ssh $USER@$host "docker load --input hlf_${system}_${new}_orderer.tar; docker load --input hlf_${system}_${new}_peer.tar"
done

