package com.cloudata.keyvalue;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class RedisIntegrationTest extends IntegrationTestBase {

    private Jedis jedis;

    @Before
    public void buildJedisClient() {
        InetSocketAddress redisSocketAddress = (InetSocketAddress) SERVERS[0].getRedisSocketAddress();

        this.jedis = new Jedis(redisSocketAddress.getHostName(), redisSocketAddress.getPort());
    }

    @Test
    public void testSetAndGet() throws Exception {

        for (int i = 1; i < 100; i++) {
            byte[] key = Integer.toString(i).getBytes();

            byte[] value = buildValue(i);

            jedis.set(key, value);
            byte[] actual = jedis.get(key);

            Assert.assertArrayEquals(value, actual);
        }
    }

    @Test
    public void testAppend() throws Exception {
        byte[] key = UUID.randomUUID().toString().getBytes();

        for (int i = 1; i < 100; i++) {
            byte[] value = new byte[] { (byte) i };

            Long newLength = jedis.append(key, value);

            Assert.assertEquals(i, newLength.longValue());
        }

        byte[] finalValue = jedis.get(key);

        for (int i = 1; i < 100; i++) {
            Assert.assertEquals(i, finalValue[i - 1]);
        }
    }

    @Test
    public void testSetAndDelete() throws Exception {
        List<byte[]> allKeys = Lists.newArrayList();
        for (int i = 1; i < 100; i++) {
            byte[] key = Integer.toString(i).getBytes();

            byte[] value = buildValue(i);

            jedis.set(key, value);

            allKeys.add(key);
        }

        Collections.shuffle(allKeys);

        for (List<byte[]> partition : Iterables.partition(allKeys, 8)) {
            byte[][] array = partition.toArray(new byte[partition.size()][]);

            Long count = jedis.del(array);
            Assert.assertEquals(partition.size(), count.longValue());
        }
    }

    @Test
    public void testIncrement() throws Exception {

        byte[] key = "INCR".getBytes();
        for (int i = 1; i < 100; i++) {
            Long value = jedis.incr(key);

            Assert.assertEquals(i, value.longValue());
        }
    }

    @Test
    public void testIncrementBy() throws Exception {

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

        byte[] key = "DECR".getBytes();
        for (int i = 1; i < 100; i++) {
            Long value = jedis.decr(key);

            Assert.assertEquals(-i, value.longValue());
        }
    }

    @Test
    public void testDecrementBy() throws Exception {

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

    @Test
    public void testExists() throws Exception {

        byte[] key = UUID.randomUUID().toString().getBytes();
        Assert.assertFalse(jedis.exists(key));

        jedis.set(key, key);

        Assert.assertTrue(jedis.exists(key));
    }

}
