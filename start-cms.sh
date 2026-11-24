#!/bin/bash
source .env
KAFKA=~/kafka_2.13-4.3.1

# create the count-min kafka folder
mkdir -p data/kafka-cms

# start the count-min redis on 6380 with the count-min module and its own save file
redis-server --port 6380 --daemonize yes --dbfilename dump-cms.rdb --loadmodule /usr/lib/redis/modules/redisbloom.so

# set up kafka on port 9094 to store its data in data/kafka-cms, and start it
sed -e "s|^log.dirs=.*|log.dirs=$PWD/data/kafka-cms|" -e "s/9092/9094/g" -e "s/9093/9095/g" $KAFKA/config/server.properties > data/kafka-cms.properties
[ -f data/kafka-cms/meta.properties ] || $KAFKA/bin/kafka-storage.sh format --standalone -t "$($KAFKA/bin/kafka-storage.sh random-uuid)" -c data/kafka-cms.properties
LOG_DIR=$PWD/data/kafka-cms-logs $KAFKA/bin/kafka-server-start.sh -daemon data/kafka-cms.properties
until $KAFKA/bin/kafka-topics.sh --list --bootstrap-server localhost:9094 > /dev/null 2>&1; do sleep 1; done

# create the clicks topic with the partitions from .env
$KAFKA/bin/kafka-topics.sh --create --if-not-exists --topic clicks --partitions "$PARTITIONS" --bootstrap-server localhost:9094

# compile the code and save the list of libraries it needs to run
mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile=data/classpath.txt

# start 2 workers per partition: one claims the lease, the other waits as backup
mkdir -p data/logs
for ((p = 0; p < PARTITIONS; p++)); do
  for w in 1 2; do
    java -cp "target/classes:$(cat data/classpath.txt)" com.helloworld.CountMinConsumer $p > data/logs/cms-worker-$p-$w.log 2>&1 &
  done
done

echo "start done!"
