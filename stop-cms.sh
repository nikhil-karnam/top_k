#!/bin/bash

# stop the workers, kafka and redis for the count-min system
pkill -f com.helloworld.CountMinConsumer
pkill -f data/kafka-cms.properties
redis-cli -p 6380 shutdown

echo "stop done!"
