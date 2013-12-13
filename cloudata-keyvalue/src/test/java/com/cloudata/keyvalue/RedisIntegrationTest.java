package com.cloudata.keyvalue;

import java.net.InetSocketAddress;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class RedisIntegrationTest extends IntegrationTestBase {

    @Test
    public void testSetAndGet() throws Exception {
        InetSocketAddress redisSocketAddress = (InetSocketAddress) SERVERS[0].getRedisSocketAddress();

        Jedis jedis = new Jedis(redisSocketAddress.getHostName(), redisSocketAddress.getPort());

        for (int i = 1; i < 100; i++) {
            byte[] key = Integer.toString(i).getBytes();

            byte[] value = buildValue(i);

            jedis.set(key, value);
            byte[] actual = jedis.get(key);

            Assert.assertArrayEquals(value, actual);
        }
    }

    @Test
    public void testIncrement() throws Exception {
        InetSocketAddress redisSocketAddress = (InetSocketAddress) SERVERS[0].getRedisSocketAddress();

        Jedis jedis = new Jedis(redisSocketAddress.getHostName(), redisSocketAddress.getPort());

        byte[] key = "INCR".getBytes();
        for (int i = 1; i < 100; i++) {
            Long value = jedis.incr(key);

            Assert.assertEquals(i, value.longValue());
        }
    }

    @Test
    public void testIncrementBy() throws Exception {
        InetSocketAddress redisSocketAddress = (InetSocketAddress) SERVERS[0].getRedisSocketAddress();

        Jedis jedis = new Jedis(redisSocketAddress.getHostName(), redisSocketAddress.getPort());

        long counter = 0;
        Random r = new Random();
        byte[] key = "INCRBY".getBytes();
        for (int i = 1; i < 100; i++) {
            long delta = r.nextInt();

            Long value = jedis.incrBy(key, delta);

            counter += delta;

            Assert.assertEquals(counter, value.longValue());
        }
    }

    @Test
    public void testDecrement() throws Exception {
        InetSocketAddress redisSocketAddress = (InetSocketAddress) SERVERS[0].getRedisSocketAddress();

        Jedis jedis = new Jedis(redisSocketAddress.getHostName(), redisSocketAddress.getPort());

        byte[] key = "DECR".getBytes();
        for (int i = 1; i < 100; i++) {
            Long value = jedis.decr(key);

            Assert.assertEquals(-i, value.longValue());
        }
    }

    @Test
    public void testDecrementBy() throws Exception {
        InetSocketAddress redisSocketAddress = (InetSocketAddress) SERVERS[0].getRedisSocketAddress();

        Jedis jedis = new Jedis(redisSocketAddress.getHostName(), redisSocketAddress.getPort());

        long counter = 0;
        Random r = new Random();
        byte[] key = "DECRBY".getBytes();
        for (int i = 1; i < 100; i++) {
            long delta = r.nextInt();

            Long value = jedis.decrBy(key, delta);

            counter -= delta;

            Assert.assertEquals(counter, value.longValue());
        }
    }

}
