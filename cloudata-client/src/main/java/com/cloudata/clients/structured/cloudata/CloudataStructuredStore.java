package com.cloudata.clients.structured.cloudata;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.clients.structured.AlreadyExistsException;
import com.cloudata.clients.structured.ReadOption;
import com.cloudata.clients.structured.StructuredStore;
import com.cloudata.clients.structured.StructuredStoreSchema;
import com.cloudata.clients.structured.VersionMismatchException;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;
import com.cloudata.structured.StructuredProtocol.StructuredActionType;
import com.cloudata.structured.StructuredProtocol.StructuredRequest;
import com.cloudata.structured.StructuredProtocol.StructuredResponse;
import com.cloudata.structured.StructuredProtocol.StructuredResponseEntry;
import com.cloudata.util.ByteStrings;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

public class CloudataStructuredStore implements StructuredStore {
    private static final Logger log = LoggerFactory.getLogger(CloudataStructuredStore.class);

    final CloudataStructuredClient client;
    final long storeId;

    public CloudataStructuredStore(CloudataStructuredClient client, long storeId) {
        this.client = client;
        this.storeId = storeId;
    }

    @Override
    public Iterator<ByteString> listKeysWithPrefix(int tablespaceId, ByteString prefix) {
        StructuredRequest.Builder b = StructuredRequest.newBuilder();

        StructuredAction.Builder ab = addCommand(b, StructuredActionType.LIST_WITH_PREFIX, tablespaceId, prefix, null);
        ab.addSuppressFields(StructuredResponseEntry.VALUE_FIELD_NUMBER);

        StructuredResponse response = executeSync(b);

        return Iterators.transform(response.getActionResponse().getEntryList().iterator(),
                new Function<StructuredResponseEntry, ByteString>() {

                    @Override
                    public ByteString apply(StructuredResponseEntry entry) {
                        return entry.getKey();
                    }

                });
    }

    private StructuredResponse executeSync(StructuredRequest.Builder b) {
        ListenableFuture<StructuredResponse> responseFuture = client.execute(b.build());
        StructuredResponse response;
        try {
            response = responseFuture.get();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return response;
    }

    private StructuredAction.Builder addCommand(StructuredRequest.Builder b, StructuredActionType action,
            int keyspaceId, ByteString key, ByteString value) {

        StructuredAction.Builder ab = b.getActionBuilder();

        ab.setStoreId(storeId);
        ab.setKeyspaceId(keyspaceId);
        if (key != null) {
            ab.setKey(key);
        }
        ab.setAction(action);
        if (value != null) {
            ab.setValue(value);
        }

        return ab;
    }

    // private int countState(StructuredActionResponse response, ActionResponseCode code) {
    // int changed = 0;
    //
    // if (response.getChildrenCount() != 0) {
    // for (StructuredActionResponse child : response.getChildrenList()) {
    // changed += countState(child, code);
    // }
    // }
    //
    // if (response.getEntryCount() != 0) {
    // for (StructuredResponseEntry entry : response.getEntryList()) {
    // if (entry.getCode() == code) {
    // changed++;
    // }
    // }
    // }
    //
    // return changed;
    // }

    public static class CloudataEntry implements Entry {
        final StructuredResponseEntry entry;

        public CloudataEntry(StructuredResponseEntry entry) {
            this.entry = entry;
        }

        @Override
        public ByteString getData() {
            return entry.getValue();
        }

        @Override
        public long getVersion() {
            try {
                return ByteStrings.hash(Hashing.md5(), getData()).asLong();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

    }

    @Override
    public Entry read(int tablespaceId, ByteString key, ReadOption... options) throws IOException {
        StructuredRequest.Builder b = StructuredRequest.newBuilder();
        StructuredAction.Builder ab = addCommand(b, StructuredActionType.STRUCTURED_GET, tablespaceId, key, null);

        setReadOptions(ab, options);

        StructuredResponse response = executeSync(b);

        StructuredActionResponse actionResponse = response.getActionResponse();
        if (actionResponse.getEntryCount() != 0) {
            assert actionResponse.getEntryCount() == 1;

            StructuredResponseEntry entry = actionResponse.getEntry(0);
            assert entry.hasValue();

            return new CloudataEntry(entry);
        }

        return null;
    }

    private void setReadOptions(StructuredAction.Builder b, ReadOption[] options) {
        if (options != null && options.length > 0) {
            for (ReadOption option : options) {
                if (option == ReadOption.NO_DATA) {
                    b.addSuppressFields(StructuredResponseEntry.VALUE_FIELD_NUMBER);
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }
    }

    @Override
    public boolean delete(int tablespaceId, ByteString key) throws IOException {
        try {
            return delete(tablespaceId, key, null);
        } catch (VersionMismatchException e) {
            throw new IOException("Protocol error (unexpected version-mismatch response)", e);
        }
    }

    private boolean delete(int tablespaceId, ByteString key, Long ifVersion) throws VersionMismatchException,
            IOException {
        StructuredRequest.Builder b = StructuredRequest.newBuilder();
        StructuredAction.Builder ab = addCommand(b, StructuredActionType.STRUCTURED_DELETE, tablespaceId, key, null);

        if (ifVersion != null) {
            ab.setIfVersion(ifVersion);
        }

        StructuredResponse response = executeSync(b);

        StructuredActionResponse actionResponse = response.getActionResponse();
        if (actionResponse.getEntryCount() == 1) {
            StructuredResponseEntry entry = actionResponse.getEntry(0);

            switch (entry.getCode()) {
            case DONE:
                return true;

            case NOT_FOUND:
                return false;

            case VERSION_MISMATCH:
                throw new VersionMismatchException();

            default:
                throw new IOException("Protocol error");
            }
        } else {
            throw new IOException("Protocol error");
        }
    }

    @Override
    public boolean delete(int tablespaceId, ByteString key, long ifVersion) throws IOException,
            VersionMismatchException {
        return delete(tablespaceId, key, ifVersion);
    }

    @Override
    public Entry create(int tablespaceId, ByteString key, ByteString data) throws IOException, AlreadyExistsException {
        try {
            return put(tablespaceId, key, data, true, null);
        } catch (VersionMismatchException e) {
            throw new IOException("Protocol error (unexpected version-mismatch response)", e);
        }
    }

    Entry put(int tablespaceId, ByteString key, ByteString data, Boolean ifNotExists, Long ifVersion)
            throws IOException, AlreadyExistsException, VersionMismatchException {
        StructuredRequest.Builder b = StructuredRequest.newBuilder();
        StructuredAction.Builder ab = addCommand(b, StructuredActionType.STRUCTURED_SET, tablespaceId, key, data);

        if (ifNotExists != null) {
            ab.setIfNotExists(ifNotExists.booleanValue());
        }

        if (ifVersion != null) {
            ab.setIfVersion(ifVersion.longValue());
        }

        ab.addSuppressFields(StructuredResponseEntry.VALUE_FIELD_NUMBER);

        StructuredResponse response = executeSync(b);

        StructuredActionResponse actionResponse = response.getActionResponse();
        if (actionResponse.getEntryCount() == 1) {
            StructuredResponseEntry entry = actionResponse.getEntry(0);

            switch (entry.getCode()) {
            case DONE:
                return new CloudataEntry(entry);

            case ALREADY_EXISTS:
                throw new AlreadyExistsException();

            case VERSION_MISMATCH:
                throw new VersionMismatchException();

            default:
                throw new IllegalStateException();
            }
        } else {
            throw new IOException("Protocol error");
        }
    }

    @Override
    public Entry put(int tablespaceId, ByteString key, ByteString data, long ifVersion) throws IOException,
            VersionMismatchException {
        try {
            return put(tablespaceId, key, data, null, ifVersion);
        } catch (AlreadyExistsException e) {
            throw new IOException("Protocol error (unexpected already-exists response)", e);
        }
    }

    @Override
    public StructuredStoreSchema getSchema() {
        return new CloudataStructuredStoreSchema(this);
    }

}
