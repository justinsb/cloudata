package com.cloudata.keyvalue;

import java.net.InetSocketAddress;

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

        byte[] key = "A".getBytes();
        for (int i = 1; i < 100; i++) {
            Long value = jedis.incr(key);

            Assert.assertEquals(i, value.intValue());
        }
    }

}
