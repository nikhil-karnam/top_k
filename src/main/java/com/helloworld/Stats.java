package com.helloworld;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.stream.IntStream;

public class Stats {

    public static void main(String[] args) {
        displayTopHourly(10);
        displayTopHourlyCms(10);
    }

    // adds up the exact minute keys for the last 60 minutes and prints the top k
    public static void displayTopHourly(int k) {
        try (RedisClient redis = RedisClient.create("localhost", 6379)) {
            String[] minuteKeys = IntStream.range(0, 60)
                .mapToObj(LocalDateTime.now(ZoneId.of("America/Chicago"))::minusMinutes)
                .map(m -> "clicks:" + m.toLocalDate() + ":" + m.getHour() + ":" + m.getMinute())
                .toArray(String[]::new);

            redis.zunionstore("top:hourly", minuteKeys);

            for (Tuple t : redis.zrangeWithScores("top:hourly", ZRangeParams.zrangeParams(0, k - 1).rev())) {
                System.out.println(t.getElement() + " " + (long) t.getScore());
            }
        }
    }

    // adds up the count-min top lists for the last 60 minutes and prints the top k
    public static void displayTopHourlyCms(int k) {
        try (RedisClient redis = RedisClient.create("localhost", 6380)) {
            String[] topLists = IntStream.range(0, 60)
                .mapToObj(LocalDateTime.now(ZoneId.of("America/Chicago"))::minusMinutes)
                .map(m -> "cms:top:clicks:" + m.toLocalDate() + ":" + m.getHour() + ":" + m.getMinute())
                .toArray(String[]::new);

            redis.zunionstore("cms:top:hourly", topLists);

            for (Tuple t : redis.zrangeWithScores("cms:top:hourly", ZRangeParams.zrangeParams(0, k - 1).rev())) {
                System.out.println(t.getElement() + " " + (long) t.getScore());
            }
        }
    }
}
