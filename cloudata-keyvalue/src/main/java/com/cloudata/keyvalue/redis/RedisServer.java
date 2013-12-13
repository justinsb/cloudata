package com.cloudata.keyvalue.redis;

import com.cloudata.keyvalue.KeyValueStore;

public class RedisServer {
    final KeyValueStore keyValueStore;

    public RedisServer(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }

    public KeyValueStore getKeyValueStore() {
        return keyValueStore;
    }

}
