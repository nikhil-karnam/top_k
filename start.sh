#!/bin/bash
source .env
KAFKA=~/kafka_2.13-4.3.1

# create the kafka folder
mkdir -p data/kafka

# start redis in the background on the port the consumer uses
redis-server --port 6379 --daemonize yes

# set up kafka to store its data in data/kafka, and start it
sed "s|^log.dirs=.*|log.dirs=$PWD/data/kafka|" $KAFKA/config/server.properties > data/kafka.properties
[ -f data/kafka/meta.properties ] || $KAFKA/bin/kafka-storage.sh format --standalone -t "$($KAFKA/bin/kafka-storage.sh random-uuid)" -c data/kafka.properties
$KAFKA/bin/kafka-server-start.sh -daemon data/kafka.properties
until $KAFKA/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 > /dev/null 2>&1; do sleep 1; done

# create the clicks topic with the partitions from .env
$KAFKA/bin/kafka-topics.sh --create --if-not-exists --topic clicks --partitions "$PARTITIONS" --bootstrap-server localhost:9092

# compile the code and save the list of libraries it needs to run
mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile=data/classpath.txt

# start 2 workers per partition: one claims the lease, the other waits as backup
mkdir -p data/logs
for ((p = 0; p < PARTITIONS; p++)); do
  for w in 1 2; do
    java -cp "target/classes:$(cat data/classpath.txt)" com.helloworld.Consumer $p > data/logs/worker-$p-$w.log 2>&1 &
  done
done

echo "start done!"
