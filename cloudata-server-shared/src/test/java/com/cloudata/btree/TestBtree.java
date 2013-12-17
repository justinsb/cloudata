package com.cloudata.btree;

import java.io.File;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.cloudata.TestUtils;
import com.cloudata.btree.operation.SetOperation;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class TestBtree {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSpaceReclamation() throws Exception {
        File data = new File(folder.getRoot(), UUID.randomUUID().toString());
        boolean uniqueKeys = true;
        PageStore pageStore = MmapPageStore.build(data, uniqueKeys);

        Btree btree = new Btree(pageStore, uniqueKeys);

        byte[] key = "A".getBytes();
        byte[] bytes = TestUtils.buildBytes(30000);

        for (int i = 1; i <= 10000; i++) {
            try (WriteTransaction txn = btree.beginReadWrite()) {
                Value value = Value.fromRawBytes(bytes);
                BtreeOperation<Void> operation = new SetOperation(value);
                txn.doAction(btree, ByteString.copyFrom(key).asReadOnlyByteBuffer(), operation);
                txn.commit();
            }
        }
    }

}
