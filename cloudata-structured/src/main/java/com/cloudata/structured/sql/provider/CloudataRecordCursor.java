/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudata.structured.sql.provider;

import java.nio.ByteBuffer;

import com.cloudata.btree.Btree;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.ReadOnlyTransaction;
import com.cloudata.structured.StructuredStore;
import com.cloudata.values.Value;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.gson.JsonObject;

public class CloudataRecordCursor implements RecordCursor {
    private final StructuredStore store;
    private final TableMetadata tableMetadata;

    // private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    // private final List<ExampleColumnHandle> columnHandles;
    // private final int[] fieldToColumnIndex;
    //
    // private final Iterator<String> lines;
    // private final long totalBytes;
    //
    // private List<String> fields;
    //
    // public CloudataRecordCursor(List<ExampleColumnHandle> columnHandles, InputSupplier<InputStream>
    // inputStreamSupplier) {
    // this.columnHandles = columnHandles;
    //
    // fieldToColumnIndex = new int[columnHandles.size()];
    // for (int i = 0; i < columnHandles.size(); i++) {
    // ExampleColumnHandle columnHandle = columnHandles.get(i);
    // fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
    // }
    //
    // try (CountingInputStream input = new CountingInputStream(inputStreamSupplier.getInput())) {
    // lines = asByteSource(inputStreamSupplier).asCharSource(StandardCharsets.UTF_8).readLines().iterator();
    // totalBytes = input.getCount();
    // } catch (IOException e) {
    // throw Throwables.propagate(e);
    // }
    // }
    //
    // @Override
    // public long getTotalBytes() {
    // return totalBytes;
    // }
    //
    // @Override
    // public long getCompletedBytes() {
    // return totalBytes;
    // }
    //
    // @Override
    // public ColumnType getType(int field) {
    // checkArgument(field < columnHandles.size(), "Invalid field index");
    // return columnHandles.get(field).getColumnType();
    // }
    //
    // @Override
    // public boolean advanceNextPosition() {
    // if (!lines.hasNext()) {
    // return false;
    // }
    // String line = lines.next();
    // fields = LINE_SPLITTER.splitToList(line);
    //
    // return true;
    // }
    //
    // private String getFieldValue(int field) {
    // checkState(fields != null, "Cursor has not been advanced yes");
    //
    // int columnIndex = fieldToColumnIndex[field];
    // return fields.get(columnIndex);
    // }
    //
    // @Override
    // public boolean getBoolean(int field) {
    // checkFieldType(field, ColumnType.BOOLEAN);
    // return Boolean.parseBoolean(getFieldValue(field));
    // }
    //
    // @Override
    // public long getLong(int field) {
    // checkFieldType(field, ColumnType.LONG);
    // return Long.parseLong(getFieldValue(field));
    // }
    //
    // @Override
    // public double getDouble(int field) {
    // checkFieldType(field, ColumnType.DOUBLE);
    // return Double.parseDouble(getFieldValue(field));
    // }
    //
    // @Override
    // public byte[] getString(int field) {
    // checkFieldType(field, ColumnType.STRING);
    // return getFieldValue(field).getBytes(Charsets.UTF_8);
    // }
    //
    // @Override
    // public boolean isNull(int field) {
    // checkArgument(field < columnHandles.size(), "Invalid field index");
    // return Strings.isNullOrEmpty(getFieldValue(field));
    // }
    //
    // private void checkFieldType(int field, ColumnType expected) {
    // ColumnType actual = getType(field);
    // checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    // }

    public CloudataRecordCursor(StructuredStore store, TableMetadata tableMetadata) {
        this.store = store;
        this.tableMetadata = tableMetadata;
    }

    @Override
    public void close() {
    }

    public void walk(Keyspace keyspace, ByteBuffer start, WalkListener listener) {
        try (Walker walker = new Walker(listener)) {
            walker.walk(start, keyspace);
        }

    }

    class Walker implements AutoCloseable {
        final WalkListener listener;

        public Walker(WalkListener listener) {
            this.listener = listener;
        }

        class Listener implements EntryListener {
            private final Keyspace keyspace;

            public Listener(Keyspace keyspace) {
                this.keyspace = keyspace;
            }

            @Override
            public boolean found(ByteBuffer key, Value value) {
                if (keyspace.contains(key)) {
                    JsonObject json = value.asJsonObject();
                    return listener.found(key, json);
                } else {
                    // TODO: Bail out
                    return true;
                }
            }
        }

        public void walk(ByteBuffer start, Keyspace keyspace) {
            Btree btree = store.getBtree();
            try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
                Listener entryListener = new Listener(keyspace);
                txn.walk(btree, start, entryListener);
            }
        }

        @Override
        public void close() {
        }

    }

    public static abstract class WalkListener {
        public abstract boolean found(ByteBuffer key, JsonObject json);
    }

    @Override
    public long getTotalBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCompletedBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnType getType(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceNextPosition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getString(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        throw new UnsupportedOperationException();
    }
}
