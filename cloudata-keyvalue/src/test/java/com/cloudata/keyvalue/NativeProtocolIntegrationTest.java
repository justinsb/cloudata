package com.cloudata.keyvalue;

import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.robotninjas.protobuf.netty.client.RpcClient;

import com.cloudata.TestUtils;
import com.cloudata.clients.keyvalue.cloudata.CloudataKeyValueClient;
import com.cloudata.clients.keyvalue.cloudata.CloudataKeyValueService;
import com.cloudata.clients.protobuf.ProtobufRpcClient;
import com.cloudata.clients.protobuf.ProtobufRpcClientProvider;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

public class NativeProtocolIntegrationTest extends IntegrationTestBase {

    private static final int SPACE_DEFAULT = 0;

    private com.cloudata.clients.keyvalue.KeyValueStore keyValueStore;

    @Before
    public void buildClient() {
        InetSocketAddress protobufSocketAddress = (InetSocketAddress) SERVERS[0].getProtobufSocketAddress();

        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        RpcClient rpcClient = new RpcClient(eventLoopGroup);
        ProtobufRpcClientProvider protobufRpcClientProvider = new ProtobufRpcClientProvider(rpcClient);
        ProtobufRpcClient protobufRpcClient = protobufRpcClientProvider.get(protobufSocketAddress);
        CloudataKeyValueClient client = new CloudataKeyValueClient(protobufRpcClient);
        CloudataKeyValueService service = new CloudataKeyValueService(client);
        ByteString storeKey = service.allocate();
        this.keyValueStore = service.get(storeKey);
    }

    @Test
    public void testSetAndGet() throws Exception {
        for (int i = 0; i < 100; i++) {
            ByteString key = ByteString.copyFromUtf8(Integer.toString(i));
            ByteString value = TestUtils.buildByteString(i);

            boolean okay = keyValueStore.putSync(SPACE_DEFAULT, key, value);
            Assert.assertTrue(okay);

            ByteString actual = keyValueStore.read(SPACE_DEFAULT, key);

            Assert.assertEquals(value, actual);
        }
    }

    @Test
    public void testPartitions() throws Exception {
        int n = 100;
        int partitions = 10;

        for (int i = 0; i < n; i++) {
            ByteString key = ByteString.copyFromUtf8(Integer.toString(i));
            ByteString value = TestUtils.buildByteString(i);

            int storeId = i / 10;
            keyValueStore.putSync(storeId, key, value);
        }

        for (int j = 0; j < partitions; j++) {
            for (int i = 0; i < n; i++) {
                ByteString key = ByteString.copyFromUtf8(Integer.toString(i));
                ByteString value = TestUtils.buildByteString(i);

                int storeId = j;

                ByteString found = keyValueStore.read(storeId, key);
                if ((i / partitions) == j) {
                    Assert.assertEquals(value, found);
                } else {
                    Assert.assertNull(found);
                }
            }
        }
    }

    @Test
    public void testSetAndDelete() throws Exception {
        List<ByteString> allKeys = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            ByteString key = ByteString.copyFromUtf8(Integer.toString(i));
            ByteString value = TestUtils.buildByteString(i);

            keyValueStore.putSync(SPACE_DEFAULT, key, value);

            allKeys.add(key);
        }

        Collections.shuffle(allKeys);

        for (List<ByteString> partition : Iterables.partition(allKeys, 8)) {
            Integer count = keyValueStore.delete(SPACE_DEFAULT, partition).get();
            Assert.assertNotNull(count);
            Assert.assertEquals(partition.size(), count.intValue());
        }
    }

}
