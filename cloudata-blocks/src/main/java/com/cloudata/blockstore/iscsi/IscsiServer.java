package com.cloudata.blockstore.iscsi;

import org.robotninjas.barge.RaftException;

import com.cloudata.blockstore.KeyValueStateMachine;
import com.cloudata.blockstore.operation.BlockStoreOperation;
import com.cloudata.btree.Keyspace;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class IscsiServer {
    final KeyValueStateMachine stateMachine;
    final long storeId;
    short nextSessionIdentifier = 1;

    public IscsiServer(KeyValueStateMachine stateMachine, long storeId) {
        this.stateMachine = stateMachine;
        this.storeId = storeId;
    }

    public <V> V doAction(BlockStoreOperation<V> operation) throws RedisException {
        try {
            return stateMachine.doAction(storeId, operation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RedisException("Interrupted during processing", e);
        } catch (RaftException e) {
            throw new RedisException("Error during processing", e);
        }
    }

    public Value get(Keyspace keyspace, ByteString key) {
        return stateMachine.get(storeId, keyspace, key);
    }

    public synchronized short assignSessionIdentifier() {
        return nextSessionIdentifier++;
    }

}
