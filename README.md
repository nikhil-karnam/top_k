Real-time, exact, exactly-once, fault-tolerant, top-k clicked videos over the last hour (rolling window), using Kafka and Redis.

## How it works

- Clicks go into a Kafka topic split into partitions (number of partitions is set in `.env`).
- Each partition has a main worker and a backup. The workers race to claim the partition, and whoever wins claims a lease in Redis for the partition (distributed lock).
- Every 10 seconds, the worker reads the partition, aggregates clicks, sends a batch to Redis, and resets its click counter.
- Kafka offset positions are saved in Redis at the same time that a batch is saved. When a worker dies, the backup worker reads the offset in Redis to know where to pick back up in Kafka. If the offset isn't saved in Redis, it cannot be updated at the same time that the batch is saved. So a worker may die after a batch is saved but before updating the offset, and this can cause a newly started worker to repeat the clicks that the old one already put into Redis. Saving it in Redis therefore prevents double counting.
- Before a batch is written, the lease in Redis is checked to determine if another worker has claimed your partition. If so, it means your worker is a zombie and the batch is rejected to avoid double counting clicks. This all happens through a Lua script. Then the zombie is made to restart.
- Each batch contains minute-videoID buckets because if minutes weren't considered, a lagging partition may add its clicks to the latest Redis minute, even though it is older. This keeps the counts exact to the minute it was clicked.
- In Redis, each key is a minute and the value contains a sorted set with video ID and count for that minute. The minute key expires after an hour of initially being set. `Stats` aggregates the keys and values for the last 60 minutes and prints the top-k for the last hour.
- For the count-min version, Redis goes through a batch's buckets and for each one, adds it to the sketch, retrieves the count estimate, adds it to a separate top-100 sorted set for that minute, and evicts video #101. Calculating the top-k aggregates these sorted sets. There is a problem: A video that happened to make #100 in a single minute but is absent in other minutes will be present in the aggregated list. But a video that made #101 for every minute will not be present in the aggregated list despite having 60x more clicks than the other video. For a top-10 report, holding 90 extra spots reduces this problem. For a top-100 report, you may want to increase the size of the sorted set.

## Run

Requires Java 21, Maven, Redis 8+, and Kafka 4.3.1 with its path set in `.env`.

`bash start.sh` and `bash start-cms.sh` to start,

send in clicks to kafka via command line as
"day":"2026-11-24,“hour”:”7”,"minute":"42","videoId":"v317"



`java -cp "target/classes:$(cat data/classpath.txt)" com.helloworld.Stats` to report top-k,

`bash stop.sh` and `bash stop-cms.sh` to stop. Folders are not deleted after stopping.

## Tested

Froze every main worker mid-stream: the backups took over, the old mains unfroze and ended up restarted themselves, and the counts came out exact.

Across one minute, 100K different videos, and 10 videos with 100 clicks, 200 clicks, 300 clicks, ... 1000 clicks: count-min achieved 93.4% accuracy for the top 10 videos (1 - total extra clicks / total clicks), 97.9% less memory for the whole minute, and perfect top-10 ranking.

Batching got through an average of 124,397 clicks per second versus 6,489 for one write per click = ~19x faster.
