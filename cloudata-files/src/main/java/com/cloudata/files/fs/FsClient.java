package com.cloudata.files.fs;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.clients.keyvalue.IfNotExists;
import com.cloudata.clients.keyvalue.KeyValueEntry;
import com.cloudata.clients.keyvalue.KeyValueStore;
import com.cloudata.clients.keyvalue.Modifier;
import com.cloudata.files.FilesModel.ChunkData;
import com.cloudata.files.FilesModel.ChunkData.Builder;
import com.cloudata.files.FilesModel.DeletedData;
import com.cloudata.files.FilesModel.InodeData;
import com.cloudata.files.blobs.BlobCache.CacheFileHandle;
import com.cloudata.files.blobs.BlobStore;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;

@Singleton
public class FsClient {
    private static final Logger log = LoggerFactory.getLogger(FsClient.class);

    private static final long ROOT_INODE = 1;

    private static final ByteString DIR_CHILDREN = ByteString.copyFrom(new byte[] { 'D' });
    private static final ByteString INODES = ByteString.copyFrom(new byte[] { 'I' });
    private static final ByteString DELETED_INODES = ByteString.copyFrom(new byte[] { 'X' });

    private static final ByteString CHUNK_PREFIX = ByteString.copyFrom(new byte[] { 'C' });

    final KeyValueStore store;

    final BlobStore blobStore;

    @Inject
    public FsClient(KeyValueStore store, BlobStore blobStore) {
        this.store = store;
        this.blobStore = blobStore;
    }

    public FsPath getRoot(FsVolume volume, FsCredentials credentials) throws IOException {
        InodeData root = readInode(volume, credentials, ROOT_INODE);
        if (root == null) {
            return null;
        }
        return new FsPath(volume, null, ByteString.EMPTY, ROOT_INODE, root);
    }

    public FsPath resolve(FsVolume volume, FsCredentials credentials, Iterable<String> path) throws IOException {
        FsPath root = getRoot(volume, credentials);
        if (root == null) {
            return null;
        }

        FsPath current = root;

        for (String token : path) {
            if (!current.isFolder()) {
                return null;
            }

            long child = findChild(current, ByteString.copyFromUtf8(token));
            if (child == 0) {
                return null;
            }
            InodeData childInode = readInode(volume, credentials, child);
            if (childInode == null) {
                log.error("Unable to read inode {}", child);
                throw new FsException("Could not read inode");
            }

            current = new FsPath(volume, current, ByteString.copyFromUtf8(token), child, childInode);
        }

        return current;
    }

    private long findChild(FsPath parent, ByteString name) throws IOException {
        ByteString key = buildDirEntryKey(parent, name);

        ByteString value = store.read(key);
        if (value == null) {
            return 0;
        }
        return ByteStrings.decodeLong(value);
    }

    private ByteString buildDirEntryKey(FsPath parent, ByteString name) {
        ByteString volumePrefix = parent.getVolume().getPrefix();
        ByteString key = volumePrefix.concat(DIR_CHILDREN).concat(ByteStrings.encode(parent.getId())).concat(name);
        return key;
    }

    private ByteString buildDirEntryKey(FsPath path) {
        ByteString volumePrefix = path.getVolume().getPrefix();
        ByteString key = volumePrefix.concat(DIR_CHILDREN).concat(ByteStrings.encode(path.getParent().getId()))
                .concat(path.getNameBytes());
        return key;
    }

    public static class DirEntry {
        final FsPath parent;
        final ByteString name;
        final long inode;
        private final FsClient client;

        public DirEntry(FsClient client, FsPath parent, ByteString name, long inode) {
            this.client = client;
            this.parent = parent;
            this.name = name;
            this.inode = inode;
        }

        public ByteString getName() {
            return name;
        }

        public long getInode() {
            return inode;
        }

        public Inode readInode() throws IOException {
            ByteString key = buildInodeKey(parent.getVolume(), inode);

            ByteString value = client.store.read(key);
            if (value == null) {
                return null;
            }
            return new Inode(name, inode, InodeData.parseFrom(value));
        }
    }

    public Iterator<DirEntry> listChildren(final FsPath fsPath) {
        FsVolume volume = fsPath.getVolume();
        final ByteString prefix = volume.getPrefix().concat(DIR_CHILDREN).concat(ByteStrings.encode(fsPath.getId()));

        return Iterators.transform(store.listEntriesWithPrefix(prefix), new Function<KeyValueEntry, DirEntry>() {
            @Override
            public DirEntry apply(KeyValueEntry entry) {
                ByteString name = entry.getKey().substring(prefix.size());
                long inode = ByteStrings.decodeLong(entry.getValue());
                return new DirEntry(FsClient.this, fsPath, name, inode);
            }

        });
    }

    private InodeData readInode(FsVolume volume, FsCredentials credentials, long inode) throws IOException {
        ByteString key = buildInodeKey(volume, inode);

        ByteString value = store.read(key);
        if (value == null) {
            return null;
        }
        return InodeData.parseFrom(value);
    }

    private static ByteString buildInodeKey(FsVolume volume, long inode) {
        ByteString key = volume.getPrefix().concat(INODES).concat(ByteStrings.encode(inode));
        return key;
    }

    public FsPath ensureFilesystem(FsVolume volume, FsCredentials credentials) throws IOException {
        FsPath path = getRoot(volume, credentials);
        if (path == null) {
            InodeData.Builder i = InodeData.newBuilder();
            int mode = 0;
            mode |= 0777;
            mode |= Inode.S_IFDIR;
            i.setMode(mode);

            /* InodeData root = */writeInode(volume, credentials, ROOT_INODE, i.build());
            path = getRoot(volume, credentials);
            Preconditions.checkState(path != null);
        }
        return path;
    }

    private InodeData writeInode(FsVolume volume, FsCredentials credentials, long inode, InodeData data)
            throws IOException {
        ByteString key = buildInodeKey(volume, inode);
        ByteString value = data.toByteString();

        store.put(key, value);
        return data;
    }

    public void createNewFile(FsPath parentPath, ByteString name, InodeData.Builder inode, ByteSource source,
            boolean overwrite) throws IOException, FsException {
        Preconditions.checkState(inode.getChunkCount() == 0);

        long length = source.size();

        ByteString hash = blobStore.put(CHUNK_PREFIX, source);
        Builder chunkBuilder = inode.addChunkBuilder();
        chunkBuilder.setHash(hash);
        chunkBuilder.setLength(length);

        inode.setLength(length);

        createNewEntry(parentPath, name, inode, overwrite);
    }

    public void createNewFolder(FsPath parentPath, ByteString name, InodeData.Builder inode, boolean overwrite)
            throws IOException, FsException {
        Preconditions.checkState(inode.getChunkCount() == 0);

        createNewEntry(parentPath, name, inode, overwrite);
    }

    private void createNewEntry(FsPath parent, ByteString name, InodeData.Builder inode, boolean overwrite)
            throws IOException, FsException {
        // TODO: We really should do this in a transaction; we can lose inodes if we crash...

        // Create the inode
        InodeData created;
        ByteString inodeKey;
        do {
            long id = assignInode();

            inode.setInode(id);

            created = inode.build();

            inodeKey = buildInodeKey(parent.getVolume(), id);
            if (store.put(inodeKey, created.toByteString(), IfNotExists.INSTANCE)) {
                log.debug("Wrote inode entry: {} = {}", id, created);

                break;
            } else {
                inode = InodeData.newBuilder(created);
                created = null;
            }
        } while (true);

        // Link the inode into the parent directory
        {
            ByteString dirEntryKey = buildDirEntryKey(parent, name);

            ByteString dirEntryValue = ByteStrings.encode(created.getInode());
            Modifier[] modifiers;
            if (overwrite) {
                // TODO: Should we record any existing key in the deleted section?
                modifiers = new Modifier[] {};
            } else {
                modifiers = new Modifier[] { IfNotExists.INSTANCE };
            }

            if (store.put(dirEntryKey, dirEntryValue, modifiers)) {
                return;
            } else {
                // Cleanup inode
                store.delete(inodeKey);

                if (!overwrite) {
                    throw new FsFileAlreadyExistsException();
                } else {
                    throw new IllegalStateException();
                }
            }
        }
    }

    final Random random = new Random();

    private long assignInode() {
        // TODO: Locality?
        while (true) {
            long id;

            synchronized (random) {
                id = random.nextLong();
            }

            // Sanity; also lets us reserve negative numbers for other things
            id = Math.abs(id);

            if (id != 0) {
                return id;
            }
        }
    }

    public FsFile openFile(FsPath fsPath) throws FsException {
        List<ChunkData> chunks = fsPath.getChunkList();

        if (chunks.size() == 0) {
            if (fsPath.isFolder()) {
                throw new IllegalArgumentException();
            } else {
                return new EmptyFsFile();
            }
        } else if (chunks.size() == 1) {
            return new SimpleFsFile(this, fsPath, chunks.get(0));
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public CacheFileHandle findChunk(ChunkData chunkData) throws IOException {
        ByteString key = chunkData.getHash();
        return blobStore.find(key);
    }

    public boolean delete(FsPath fsPath) throws IOException {
        // Store a deletion record
        // Allows for more efficient GC, and for undelete
        // TODO: Support GC

        long id = fsPath.getId();
        DeletedData.Builder b = DeletedData.newBuilder();
        b.setInode(id);
        for (FsPath entry : fsPath.getPathComponentList()) {
            b.addPath(entry.getNameBytes());
        }
        ByteString deletedValue = b.build().toByteString();

        ByteString volumePrefix = fsPath.getVolume().getPrefix();

        while (true) {
            ByteString key = volumePrefix.concat(DELETED_INODES).concat(ByteStrings.encode(id))
                    .concat(ByteStrings.encode(System.currentTimeMillis()));

            if (store.put(key, deletedValue, IfNotExists.INSTANCE)) {
                break;
            }
        }

        // Unlink the entry from the name tree
        {
            ByteString key = buildDirEntryKey(fsPath);

            return store.delete(key);
        }
    }

    public void move(FsPath fsPath, FsPath newParentFsPath, ByteString newName) throws IOException {
        long id = fsPath.getId();

        ByteString oldKey = buildDirEntryKey(fsPath);
        ByteString newKey = buildDirEntryKey(newParentFsPath, newName);

        // Create the new dir entry, delete the old one
        {
            ByteString dirEntryValue = ByteStrings.encode(id);
            Modifier[] modifiers = new Modifier[] { IfNotExists.INSTANCE };

            if (!store.put(newKey, dirEntryValue, modifiers)) {
                throw new FsFileAlreadyExistsException();
            }
        }

        store.delete(oldKey);
    }
}
