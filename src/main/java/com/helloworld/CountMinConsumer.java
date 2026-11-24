package com.helloworld;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

public class CountMinConsumer {

    public static void main(String[] args) throws Exception {
        int myPartition = Integer.parseInt(args[0]);
        String heartbeatKey = "heartbeat:" + myPartition;
        String bookmarkKey = "bookmark:" + myPartition;
        String workerId = UUID.randomUUID().toString();

        // create a connection to redis for heartbeats and batch jobs
        Jedis heartbeat = new Jedis("localhost", 6380);
        Jedis batchJob = new Jedis("localhost", 6380);

        // if the partition isn't taken, claim it with a lease key with this worker's id
        // i hate distributed locks
        while (heartbeat.set(heartbeatKey, workerId, SetParams.setParams().nx().ex(10)) == null) {
            // if the partition is taken, wait 15 seconds and try to claim it again
            Thread.sleep(15000);
        }

        // if you've gotten to this point in the code, the partition is yours.
        // refresh every 5 seconds to keep lease.
        Thread refresher = new Thread(() -> {
            while (true) {
                heartbeat.expire(heartbeatKey, 10);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        refresher.setDaemon(true);
        refresher.start();

        // sets up kafka's reader for the assigned partition, starting at the bookmark
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> reader = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition("clicks", myPartition);
        reader.assign(List.of(tp));
        long bookmark = Long.parseLong(Objects.requireNonNullElse(batchJob.get(bookmarkKey), "0"));
        reader.seek(tp, bookmark);

        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, Long> counts = new HashMap<>();
        long lastSend = System.currentTimeMillis();

        while (true) {
            // reads clicks, counts them, sorted into minute-video buckets
            for (ConsumerRecord<String, String> r : reader.poll(Duration.ofSeconds(1))) {
                JsonNode c = mapper.readTree(r.value());
                counts.merge("clicks:" + c.get("day").asText() + ":" + c.get("hour").asText() + ":" + c.get("minute").asText() + "|" + c.get("videoId").asText(), 1L, Long::sum);
                bookmark = r.offset() + 1;
            }

            if (System.currentTimeMillis() - lastSend >= 10_000) {
                // prepares batch
                List<String> batch = new ArrayList<>();

                batch.add(workerId);
                batch.add(String.valueOf(bookmark));
                for (Map.Entry<String, Long> e : counts.entrySet()) {
                    String[] p = e.getKey().split("\\|");
                    batch.add(p[0]);
                    batch.add(p[1]);
                    batch.add(String.valueOf(e.getValue()));
                }

                // puts a batch and updates the bookmark at once
                // unless you're a zombie (lease is not yours no more), then it rejects the batch
                String script = """
                    local lease = KEYS[1]
                    local bookmarkKey = KEYS[2]
                    local workerId = ARGV[1]
                    local bookmark = ARGV[2]
                    local batch = {}
                    for i = 3, #ARGV do
                        table.insert(batch, ARGV[i])
                    end

                    if redis.call('GET', lease) ~= workerId then
                        return 0
                    end

                    redis.call('SET', bookmarkKey, bookmark)

                    for i = 1, #batch, 3 do
                        local minute = batch[i]
                        local video = batch[i + 1]
                        local count = batch[i + 2]

                        if redis.call('EXISTS', 'cms:' .. minute) == 0 then
                            redis.call('CMS.INITBYDIM', 'cms:' .. minute, 2000, 5)
                            redis.call('ZADD', 'cms:top:' .. minute, 0, video)

                            redis.call('EXPIRE', 'cms:' .. minute, 3600)
                            redis.call('EXPIRE', 'cms:top:' .. minute, 3600)
                        end

                        local estimate = redis.call('CMS.INCRBY', 'cms:' .. minute, video, count)[1]
                        redis.call('ZADD', 'cms:top:' .. minute, estimate, video)
                        redis.call('ZREMRANGEBYRANK', 'cms:top:' .. minute, 0, -101)
                    end

                    return 1
                """;

                Object result = batchJob.eval(script, List.of(heartbeatKey, bookmarkKey), batch);
                // if you're a zombie, restart
                if (Long.valueOf(0).equals(result)) {
                    refresher.interrupt();
                    heartbeat.close();
                    batchJob.close();
                    reader.close();

                    main(args);
                    return;
                }

                // reset the aggregates and the time since last batch
                counts.clear();
                lastSend = System.currentTimeMillis();
            }
        }
    }
}
