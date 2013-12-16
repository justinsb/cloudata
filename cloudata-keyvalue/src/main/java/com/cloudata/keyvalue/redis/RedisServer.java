package com.cloudata.keyvalue.redis;

import java.nio.ByteBuffer;

import org.robotninjas.barge.RaftException;

import com.cloudata.keyvalue.KeyValueStateMachine;
import com.cloudata.keyvalue.btree.operation.KeyOperation;
import com.google.protobuf.ByteString;

public class RedisServer {
    final KeyValueStateMachine stateMachine;
    final long storeId;

    public RedisServer(KeyValueStateMachine stateMachine, long storeId) {
        this.stateMachine = stateMachine;
        this.storeId = storeId;
    }

    public <V> V doAction(ByteString key, KeyOperation<V> operation) throws RedisException {
        try {
            return stateMachine.doAction(storeId, key, operation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RedisException("Interrupted during processing", e);
        } catch (RaftException e) {
            throw new RedisException("Error during processing", e);
        }
    }

    public ByteBuffer get(ByteString key) {
        return stateMachine.get(storeId, key.asReadOnlyByteBuffer());
    }

}
