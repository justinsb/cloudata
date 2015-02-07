package com.cloudata.git.jgit;

import java.io.IOException;

import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.ObjectIdRef.PeeledNonTag;
import org.eclipse.jgit.lib.ObjectIdRef.Unpeeled;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Ref.Storage;
import org.eclipse.jgit.lib.SymbolicRef;
import org.eclipse.jgit.util.RefList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.Modifier;
import com.cloudata.datastore.UniqueIndexViolation;
import com.cloudata.datastore.WhereModifier;
import com.cloudata.git.GitModel.RefData;
import com.cloudata.git.GitModel.RepositoryData;
import com.google.protobuf.ByteString;

public class CloudRefDatabase extends DfsRefDatabase {
  static final Logger log = LoggerFactory.getLogger(CloudRefDatabase.class);

  private final DataStore dataStore;

  // private final ByteString prefix;

  // static final int SPACE_REFS = 0;

  public CloudRefDatabase(CloudDfsRepository repository, DataStore dataStore) {
    super(repository);
    this.dataStore = dataStore;
    // this.store = refsPath.store;
    // this.prefix = refsPath.key;
  }

  @Override
  protected CloudDfsRepository getRepository() {
    return (CloudDfsRepository) super.getRepository();
  }

  @Override
  protected RefCache scanAllRefs() throws IOException {
    RefList.Builder<Ref> ids = new RefList.Builder<Ref>();
    RefList.Builder<Ref> sym = new RefList.Builder<Ref>();

    try {
      RefData.Builder matcher = RefData.newBuilder();
      matcher.setRepositoryId(getRepository().getData().getRepositoryId());

      for (RefData refData : dataStore.find(matcher.build())) {
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
    RefData newModel = toModel(getRepository().getData(), newRef);

    try {
      if (oldRef == null || oldRef.getStorage() == Storage.NEW) {
        try {
          dataStore.insert(newModel);
        } catch (UniqueIndexViolation e) {
          return false;
        }
        return true;
      }

      CloudRef cur = find(newRef.getName());
      if (cur == null || !cur.matches(oldRef)) {
        // Current version already out of data
        return false;
      }

      Modifier whereUnchanged = WhereModifier.create(cur.data);
      return dataStore.update(newModel, whereUnchanged);
    } catch (IOException e) {
      log.warn("Error during ref update", e);
      throw new IOException("Error creating reference", e);
    } catch (Exception e) {
      log.error("Unexpected error during ref update", e);
      throw new IOException("Error creating reference", e);
    }

  }

  class CloudRef {
    final RefData data;

    public CloudRef(RefData data) {
      this.data = data;
    }

    public boolean matches(Ref ref) {
      RefData d2 = toModel(getRepository().getData(), ref);
      return data.equals(d2);
    }

    public boolean delete() throws IOException {
      try {
        Modifier whereUnchanged = WhereModifier.create(data);
        if (!dataStore.delete(data, whereUnchanged)) {
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
    RefData.Builder matcher = RefData.newBuilder();
    matcher.setRepositoryId(getRepository().getData().getRepositoryId());
    matcher.setName(name);

    try {
      RefData ref = dataStore.findOne(matcher.build());
      if (ref == null) {
        return null;
      }
      return new CloudRef(ref);
    } catch (IOException e) {
      throw new IOException("Error while reading reference", e);
    }
  }

  static RefData toModel(RepositoryData repositoryData, Ref ref) {
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

    data.setRepositoryId(repositoryData.getRepositoryId());
    return data.build();
  }

  private Ref fromModel(RefData refData) {
    if (refData.hasTargetName()) {
      SymbolicRef ref = new SymbolicRef(refData.getName(), new ObjectIdRef.Unpeeled(Storage.NEW,
          refData.getTargetName(), null));
      return ref;
    } else if (refData.hasObjectId()) {
      Ref ref = new ObjectIdRef.PeeledNonTag(Storage.PACKED, refData.getName(), ObjectId.fromRaw(refData.getObjectId()
          .toByteArray()));
      return ref;
    } else {
      throw new IllegalStateException();
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
