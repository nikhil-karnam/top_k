#!/bin/bash
KAFKA=~/kafka_2.13-4.3.1

# stop all the workers
pkill -f com.helloworld.Consumer

# stop kafka
$KAFKA/bin/kafka-server-stop.sh

# stop redis
redis-cli -p 6379 shutdown

echo "stop done!"
