#!/bin/bash 
 set -e

docker stack rm xox 
docker stack deploy --resolve-image never --compose-file docker-compose.yaml xox  

cnt=0
while true; do 
    wait=$(docker service list | grep 0/1 | wc -l)
    if [ $wait -eq 0 ]; then 
        break;
    fi 
    sleep 2
    let cnt=cnt+1
    echo "waiting... $cnt"
    docker service list | awk 'NR == 1 {next} {printf "%s\t%s\n", $4, $2}'
    if [ $cnt -gt 30 ]; then 
        let cnt=-1
        break 
    fi
done
if [ $cnt -eq -1 ]; then 
    docker stack rm xox 
    echo "something failed"
    exit 1
fi 

cli=$(docker ps | grep cli | awk '{print $1}')
tape=$(docker ps | grep tape | awk '{print $1}')

docker exec $cli bash scripts/script.sh
sleep 5

docker exec $tape tape -c config.yaml --e2e --txtype put --endorser_group 4 --number 10000 --seed 1317 --rate 30000 --orderer_client 20 --num_of_conn 8 --client_per_conn 4 --burst 30000

docker cp $tape:log.transactions .

sleep 2