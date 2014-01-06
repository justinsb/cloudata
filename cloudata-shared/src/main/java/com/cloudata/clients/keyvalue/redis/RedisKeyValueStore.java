package com.cloudata.clients.keyvalue.redis;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import com.cloudata.clients.keyvalue.IfNotExists;
import com.cloudata.clients.keyvalue.IfVersion;
import com.cloudata.clients.keyvalue.KeyValueEntry;
import com.cloudata.clients.keyvalue.KeyValueStore;
import com.cloudata.clients.keyvalue.Modifier;
import com.cloudata.pool.Pool;
import com.cloudata.pool.Pooled;
import com.cloudata.pool.SimplePool;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;

public class RedisKeyValueStore implements KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(RedisKeyValueStore.class);

    final Pool<RedisClient> clientPool;
    final ListeningExecutorService executor;

    /**
     * Jedis doesn't implements Closaeable :-(
     */
    class RedisClient implements Closeable {
        final Jedis jedis;

        public RedisClient(Jedis jedis) {
            this.jedis = jedis;
        }

        public Jedis getClient() {
            return jedis;
        }

        @Override
        public void close() throws IOException {
            jedis.quit();
        }
    }

    public RedisKeyValueStore(final InetSocketAddress address) {
        this(address, MoreExecutors.listeningDecorator(Executors.newCachedThreadPool()));
    }

    public RedisKeyValueStore(final InetSocketAddress address, ListeningExecutorService executor) {
        this.executor = executor;
        this.clientPool = new SimplePool<RedisClient>(new Callable<RedisClient>() {
            @Override
            public RedisClient call() throws Exception {
                Jedis jedis = new Jedis(address.getAddress().getHostAddress(), address.getPort());
                return new RedisClient(jedis);
            }
        });
    }

    @Override
    public Iterator<ByteString> listKeysWithPrefix(int space, ByteString prefix) {
        byte[] pattern = new byte[prefix.size() + 1];
        prefix.copyTo(pattern, 0);
        pattern[prefix.size()] = '*';

        try (Pooled<RedisClient> lease = clientPool.borrow()) {
            Jedis client = lease.get().getClient();
            if (client.getDB().intValue() != space) {
                client.select(space);
            }

            return Iterables.transform(client.keys(pattern), new Function<byte[], ByteString>() {

                @Override
                public ByteString apply(byte[] input) {
                    return ByteString.copyFrom(input);
                }

            }).iterator();
        }
    }

    @Override
    public Iterator<KeyValueEntry> listEntriesWithPrefix(final int space, ByteString prefix) {
        Iterator<ByteString> keys = listKeysWithPrefix(space, prefix);

        log.warn("Redis listEntriesWithPrefix makes N calls");
        return Iterators.transform(keys, new Function<ByteString, KeyValueEntry>() {

            @Override
            public KeyValueEntry apply(ByteString key) {
                ByteString value;
                try {
                    value = read(space, key);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                if (value == null) {
                    return null;
                }
                return new KeyValueEntry(key, value);
            }

        });
    }

    @Override
    public ByteString read(int space, ByteString key) throws IOException {
        try (Pooled<RedisClient> lease = clientPool.borrow()) {
            Jedis client = lease.get().getClient();
            if (client.getDB().intValue() != space) {
                client.select(space);
            }

            byte[] value = client.get(key.toByteArray());
            if (value == null) {
                return null;
            }
            return ByteString.copyFrom(value);
        }
    }

    @Override
    public boolean delete(int space, ByteString key, Modifier... modifiers) throws IOException {
        try (Pooled<RedisClient> lease = clientPool.borrow()) {
            Jedis client = lease.get().getClient();

            if (client.getDB().intValue() != space) {
                client.select(space);
            }

            byte[] keyBytes = key.toByteArray();
            if (modifiers == null || modifiers.length == 0) {
                long deleted = client.del(keyBytes);
                return deleted == 1;
            }

            Object ifVersion = null;
            for (Modifier modifier : modifiers) {
                if (modifier instanceof IfVersion) {
                    Preconditions.checkArgument(ifVersion == null);
                    ifVersion = ((IfVersion) modifier).version;
                    Preconditions.checkArgument(ifVersion != null);
                } else {
                    Preconditions.checkArgument(false);
                }
            }

            Preconditions.checkState(ifVersion != null);

            try (Atomic atomic = Atomic.start(client)) {
                client.watch(keyBytes);

                byte[] existing = client.get(keyBytes);
                if (existing == null) {
                    return false;
                }

                if (!Arrays.equals(existing, ((ByteString) ifVersion).toByteArray())) {
                    return false;
                }

                Transaction writes = atomic.startWrites();

                Response<Long> deletedCount = writes.del(keyBytes);

                if (!atomic.commit()) {
                    return false;
                }

                return deletedCount.get().longValue() == 1;
            }
        }

    }

    static class Atomic implements AutoCloseable {
        final Jedis client;
        private Transaction transaction;
        boolean clearedWrites = false;

        public Atomic(Jedis client) {
            this.client = client;
        }

        public boolean commit() {
            Preconditions.checkState(transaction != null);
            List<Object> result = transaction.exec();
            clearedWrites = true;
            transaction = null;

            if (result == null) {
                return false;
            }

            return true;
        }

        private static Atomic start(Jedis client) {
            return new Atomic(client);
        }

        public Transaction startWrites() {
            Preconditions.checkState(transaction == null);
            transaction = client.multi();
            return transaction;
        }

        @Override
        public void close() {
            if (transaction != null) {
                transaction.discard();
                clearedWrites = true;
                transaction = null;
            }

            if (!clearedWrites) {
                client.unwatch();
                clearedWrites = true;
            }
        }
    }

    @Override
    public boolean putSync(int space, ByteString key, ByteString value, Modifier... modifiers) throws IOException {
        try (Pooled<RedisClient> lease = clientPool.borrow()) {
            Jedis client = lease.get().getClient();
            if (client.getDB().intValue() != space) {
                client.select(space);
            }

            byte[] keyBytes = key.toByteArray();
            byte[] valueBytes = value.toByteArray();

            if (modifiers == null || modifiers.length == 0) {
                throwIfNotOk(client.set(keyBytes, valueBytes));
                return true;
            }

            Object ifVersion = null;
            boolean ifNotExists = false;

            for (Modifier modifier : modifiers) {
                if (modifier instanceof IfVersion) {
                    Preconditions.checkArgument(ifVersion == null);
                    ifVersion = ((IfVersion) modifier).version;
                    Preconditions.checkArgument(ifVersion != null);
                } else if (modifier instanceof IfNotExists) {
                    Preconditions.checkArgument(ifNotExists == false);
                    ifNotExists = true;
                } else {
                    Preconditions.checkArgument(false);
                }
            }

            if (ifNotExists) {
                Preconditions.checkState(ifVersion == null);
                long setnx = client.setnx(keyBytes, valueBytes);
                if (setnx == 1) {
                    return true;
                } else if (setnx == 0) {
                    return false;
                } else {
                    throw new IOException("Expected 0 or 1");
                }
            }

            Preconditions.checkState(ifVersion != null);

            try (Atomic atomic = Atomic.start(client)) {
                client.watch(keyBytes);

                byte[] existing = client.get(keyBytes);
                if (existing == null) {
                    return false;
                }

                if (!Arrays.equals(existing, ((ByteString) ifVersion).toByteArray())) {
                    return false;
                }

                Transaction writes = atomic.startWrites();

                Response<String> setResponse = writes.set(keyBytes, valueBytes);

                if (!atomic.commit()) {
                    return false;
                }

                throwIfNotOk(setResponse.get());
                return true;
            }
        }
    }

    private void throwIfNotOk(String ret) throws IOException {
        if ("OK".equals(ret)) {
            return;
        }

        throw new IOException("Expected OK");
    }

    @Override
    public ListenableFuture<Boolean> putAsync(final int space, final ByteString key, final ByteString value,
            final Modifier... modifiers) {
        return executor.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return putSync(space, key, value, modifiers);
            }

        });
    }

    @Override
    public ListenableFuture<Integer> delete(final int keyspaceId, List<ByteString> keys) {
        final byte[][] keyBytes = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            keyBytes[i] = keys.get(i).toByteArray();
        }

        return executor.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                try (Pooled<RedisClient> lease = clientPool.borrow()) {
                    Jedis client = lease.get().getClient();

                    if (client.getDB().intValue() != keyspaceId) {
                        client.select(keyspaceId);
                    }

                    Long deleted = client.del(keyBytes);
                    if (deleted == null) {
                        return null;
                    }
                    return deleted.intValue();
                }
            }
        });
    }

}
