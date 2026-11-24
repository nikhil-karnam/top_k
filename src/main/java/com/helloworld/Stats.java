package com.helloworld;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.stream.IntStream;

public class Stats {

    public static void main(String[] args) {
        displayTopHourly(10);
    }

    // adds up the minute keys for the last 60 minutes and prints the top k
    public static void displayTopHourly(int k) {
        try (Jedis redis = new Jedis("localhost", 6379)) {
            LocalDateTime now = LocalDateTime.now(ZoneId.of("America/Chicago"));

            // list of minutes in the last hour as redis key names, eg: clicks:2026-11-24:0:24
            String[] minuteKeys = IntStream.range(0, 60)
                .mapToObj(i -> now.minusMinutes(i))
                .map(m -> "clicks:" + m.toLocalDate() + ":" + m.getHour() + ":" + m.getMinute())
                .toArray(String[]::new);

            // aggregate the keys from the last hour and save it as a new redis key
            redis.zunionstore("top:hourly", minuteKeys);

            for (Tuple t : redis.zrangeWithScores("top:hourly", ZRangeParams.zrangeParams(0, k - 1).rev())) {
                System.out.println(t.getElement() + " " + (long) t.getScore());
            }
        }
    }
}
