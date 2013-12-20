package com.cloudata.git.jgit;

import java.io.IOException;
import java.util.Iterator;

import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.ObjectIdRef.PeeledNonTag;
import org.eclipse.jgit.lib.ObjectIdRef.Unpeeled;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Ref.Storage;
import org.eclipse.jgit.lib.SymbolicRef;
import org.eclipse.jgit.util.RefList;

import com.cloudata.clients.keyvalue.IfNotExists;
import com.cloudata.clients.keyvalue.IfVersion;
import com.cloudata.clients.keyvalue.KeyValueEntry;
import com.cloudata.clients.keyvalue.KeyValuePath;
import com.cloudata.clients.keyvalue.KeyValueStore;
import com.cloudata.git.GitModel.RefData;
import com.google.protobuf.ByteString;

public class CloudRefDatabase extends DfsRefDatabase {
    private final KeyValueStore store;
    private final ByteString prefix;

    public CloudRefDatabase(CloudDfsRepository repository, KeyValuePath refsPath) {
        super(repository);
        this.store = refsPath.store;
        this.prefix = refsPath.key;
    }

    @Override
    protected RefCache scanAllRefs() throws IOException {
        RefList.Builder<Ref> ids = new RefList.Builder<Ref>();
        RefList.Builder<Ref> sym = new RefList.Builder<Ref>();

        try {
            Iterator<KeyValueEntry> entriesWithPrefix = store.listEntriesWithPrefix(prefix);
            while (entriesWithPrefix.hasNext()) {
                KeyValueEntry entry = entriesWithPrefix.next();
                ByteString value = entry.getValue();
                if (value == null) {
                    // Key deleted concurrently
                    continue;
                }

                RefData refData = RefData.parseFrom(value);

                Ref ref = fromModel(refData);

                if (ref.isSymbolic()) {
                    sym.add(ref);
                }
                ids.add(ref);
            }
        } catch (Exception e) {
            throw new IOException("Error reading tags", e);
        }

        ids.sort();
        sym.sort();
        return new RefCache(ids.toRefList(), sym.toRefList());
    }

    @Override
    protected boolean compareAndPut(Ref oldRef, Ref newRef) throws IOException {
        String name = newRef.getName();
        ByteString refPath = getRefPath(name);

        if (oldRef == null || oldRef.getStorage() == Storage.NEW) {
            CloudRef newCloudRef = new CloudRef(toModel(newRef), refPath, null);
            return newCloudRef.putIfAbsent();
        }

        CloudRef cur = find(name);
        if (cur != null && cur.matches(oldRef)) {
            return cur.replace(toModel(newRef));
        } else {
            return false;
        }

    }

    private ByteString getRefPath(String name) {
        return prefix.concat(ByteString.copyFromUtf8(name));
    }

    class CloudRef {
        final ByteString key;
        final Object version;

        final RefData data;

        public CloudRef(RefData data, ByteString key, Object version) {
            this.data = data;
            this.key = key;
            this.version = version;
        }

        public boolean replace(RefData newRef) throws IOException {
            try {
                ByteString value = newRef.toByteString();

                if (!store.put(key, value, new IfVersion(version))) {
                    return false;
                }
                return true;
            } catch (IOException e) {
                throw new IOException("Error replacing node", e);
            }
        }

        public boolean putIfAbsent() throws IOException {
            try {
                ByteString bytes = data.toByteString();

                return store.put(key, bytes, IfNotExists.INSTANCE);
            } catch (IOException e) {
                throw new IOException("Error creating reference", e);
            }
        }

        public boolean matches(Ref ref) {
            RefData d2 = toModel(ref);
            return data.equals(d2);
        }

        public boolean delete() throws IOException {
            try {
                if (!store.delete(key, new IfVersion(version))) {
                    return false;
                }
                return true;
            } catch (IOException e) {
                throw new IOException("Error deleting node", e);
            }
        }
    }

    @Override
    protected boolean compareAndRemove(Ref oldRef) throws IOException {
        String name = oldRef.getName();
        CloudRef cur = find(name);
        if (cur != null && cur.matches(oldRef)) {
            return cur.delete();
        } else {
            return false;
        }
    }

    private CloudRef find(String name) throws IOException {
        ByteString refPath = getRefPath(name);

        try {
            ByteString value = store.read(refPath);

            if (value == null) {
                return null;
            }

            RefData ref = RefData.parseFrom(value);
            return new CloudRef(ref, refPath, value);
        } catch (IOException e) {
            throw new IOException("Error while reading reference", e);
        }
    }

    static RefData toModel(Ref ref) {
        RefData.Builder data = RefData.newBuilder();

        String name = ref.getName();
        if (name != null) {
            data.setName(name);
        }

        if (ref instanceof SymbolicRef) {
            Ref target = ref.getTarget();
            if (target == null) {
                throw new IllegalStateException();
            }

            if (target instanceof Unpeeled) {
                String targetName = target.getName();
                if (targetName != null) {
                    data.setTargetName(targetName);
                } else {
                    throw new IllegalStateException();
                }
            } else {
                throw new IllegalArgumentException();
            }
        } else if (ref instanceof PeeledNonTag) {
            ObjectId objectId = ref.getObjectId();
            if (objectId != null) {
                byte[] buf = new byte[20];
                objectId.copyRawTo(buf, 0);
                data.setObjectId(ByteString.copyFrom(buf));
            } else {
                throw new IllegalArgumentException();
            }
        } else {
            throw new IllegalArgumentException();
        }

        return data.build();
    }

    private Ref fromModel(RefData refData) {
        if (refData.hasTargetName()) {
            SymbolicRef ref = new SymbolicRef(refData.getName(), new ObjectIdRef.Unpeeled(Storage.NEW,
                    refData.getTargetName(), null));
            return ref;
        } else {
            if (refData.hasObjectId()) {
                Ref ref = new ObjectIdRef.PeeledNonTag(Storage.PACKED, refData.getName(), ObjectId.fromRaw(refData
                        .getObjectId().toByteArray()));
                return ref;
            } else {
                throw new IllegalStateException();
            }
        }
    }

    // private boolean eq(Ref a, Ref b) {
    // if (a.getObjectId() == null && b.getObjectId() == null)
    // return true;
    // if (a.getObjectId() != null)
    // return a.getObjectId().equals(b.getObjectId());
    // return false;
    // }
}
