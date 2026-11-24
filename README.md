# top_k

Real-time top K most-clicked videos over the last hour, using Kafka and Redis, with every click counted exactly once.

## How it works

- Clicks go into a Kafka topic split into partitions (set in `.env`).
- Each partition has a main worker and a backup. The workers race to claim the partition, and whoever wins claims a lease in Redis for the partition (distributed lock).
- Every 10 seconds, the worker reads the partition, aggregates clicks, sends a batch to Redis, and resets its click counter.
- Kafka offset positions are saved in Redis at the same time that a batch is saved. When a worker dies, the backup worker reads the offset in Redis to know where to pick back up in Kafka. If the offset isn't saved in Redis, it cannot be updated at the same time that the batch is saved. So a worker may die after a batch is saved but before updating the offset, and this can cause a newly started worker to repeat the clicks that the old one already put into Redis. Saving it in Redis therefore prevents double counting.
- Before a batch is written, the lease in Redis is checked to determine if another worker has claimed your partition. If so, it means your worker is a zombie and the batch is rejected to avoid double counting clicks. This all happens through a Lua script. Then the zombie is made to restart.
- In Redis, each key is a minute and the value contains a sorted set with video ID and count for that minute. The minute key expires after an hour of initially being set.
- `Stats` aggregates the keys and values of the last 60 minutes and prints the top K.

## Run

Requires Java 21, Maven, Redis 7+, and Kafka 4.3.1 at `~/kafka_2.13-4.3.1`.

```
./start.sh
java -cp "target/classes:$(cat data/classpath.txt)" com.helloworld.Stats
./stop.sh
```

## Tested

Froze every main worker mid-stream: the backups took over, the old mains unfroze and ended up restarted themselves, and the counts came out exact.
