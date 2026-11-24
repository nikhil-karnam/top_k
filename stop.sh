#!/bin/bash

# stop the workers, kafka and redis for the exact system
pkill -f com.helloworld.Consumer
pkill -f data/kafka.properties
redis-cli -p 6379 shutdown

echo "stop done!"
