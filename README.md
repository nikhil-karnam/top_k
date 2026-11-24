# top_k

Real-time top K most-clicked videos over the last hour, using Kafka and Redis, with every click counted exactly once.

## How it works

- Clicks go into a Kafka topic split into partitions (set in `.env`).
- Each partition has a main worker and a backup. The main holds a lease in Redis that expires unless it keeps renewing it; if it dies, the backup claims the lease.
- Every 10 seconds, a worker writes its per-minute, per-video counts and its Kafka bookmark to Redis in one Lua script, so a new worker always picks up exactly where the last one left off.
- The script rejects the write if the lease isn't the worker's own, so a frozen worker that wakes up after being replaced can't double count.
- Each minute is a Redis sorted set that expires after an hour. `Stats` adds up the last 60 and prints the top K.

## Run

Requires Java 21, Maven, Redis 7+, and Kafka 4.3.1 at `~/kafka_2.13-4.3.1`.

```
./start.sh
java -cp "target/classes:$(cat data/classpath.txt)" com.helloworld.Stats
./stop.sh
```

## Tested

Froze every main worker mid-stream: the backups took over, the old mains stopped themselves, and the counts came out exact.
