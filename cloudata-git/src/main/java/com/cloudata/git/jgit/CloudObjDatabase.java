package com.cloudata.git.jgit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.DfsReaderOptions;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.objectstore.ObjectInfo;
import com.cloudata.objectstore.ObjectStorePath;
import com.google.common.collect.Lists;

public class CloudObjDatabase extends DfsObjDatabase {
    final Logger log = LoggerFactory.getLogger(CloudObjDatabase.class);

    final ObjectStorePath blobPath;
    final File tempDir;

    CloudObjDatabase(CloudDfsRepository repo, ObjectStorePath blobPath, File tempDir) {
        super(repo, new DfsReaderOptions());
        this.blobPath = blobPath;
        this.tempDir = tempDir;
    }

    @Override
    protected synchronized List<DfsPackDescription> listPacks() throws IOException {
        List<String> suffixes = Lists.newArrayList();
        try {
            List<ObjectInfo> children = blobPath.listChildren(false);
            if (children != null) {
                for (ObjectInfo child : children) {
                    if (child.isSubdir()) {
                        continue;
                    }

                    String childPath = child.getPath();
                    String parentPath = blobPath.getPath();

                    String suffix = childPath;
                    if (suffix.startsWith(parentPath)) {
                        suffix = suffix.substring(parentPath.length());
                    } else {
                        assert false;
                    }
                    if (suffix.startsWith("/")) {
                        suffix = suffix.substring(1);
                    }
                    suffixes.add(suffix);
                }
            }
        } catch (IOException e) {
            throw new IOException("Error listing blobs", e);
        }
        List<DfsPackDescription> ret = Lists.newArrayList();
        DfsRepositoryDescription repoDesc = getRepository().getDescription();
        for (String suffix : suffixes) {
            if (!suffix.endsWith(".pack")) {
                continue;
            }
            int lastDot = suffix.lastIndexOf('.');
            if (lastDot == -1) {
                continue;
            }

            String extension = suffix.substring(lastDot + 1);
            if (!extension.equals("pack")) {
                throw new IllegalStateException();
            }

            String name = suffix; // .substring(0, lastDot);

            CloudDfsPackDescription pack = new CloudDfsPackDescription(name, repoDesc);
            ret.add(pack);
        }
        return ret;
    }

    @Override
    protected DfsPackDescription newPack(PackSource source) {
        String id = UUID.randomUUID().toString();

        String name = "pack-" + id + "-" + source.name();

        CloudDfsPackDescription desc = new CloudDfsPackDescription(name, getRepository().getDescription());
        return desc.setPackSource(source);
    }

    @Override
    protected synchronized void commitPackImpl(Collection<DfsPackDescription> desc,
            Collection<DfsPackDescription> replace) {
        // List<DfsPackDescription> n;
        // n = new ArrayList<DfsPackDescription>(desc.size() + packs.size());
        // n.addAll(desc);
        // n.addAll(packs);
        // if (replace != null)
        // n.removeAll(replace);
        // packs = n;

        // TODO: Should we "activate" packs??

        if (replace != null) {
            for (DfsPackDescription pack : replace) {
                ObjectStorePath swiftPath = getBlobPath((CloudDfsPackDescription) pack, PackExt.PACK);
                try {
                    swiftPath.delete();
                } catch (IOException e) {
                    log.warn("Error deleting file during commit {}", swiftPath);
                }
            }
        }
    }

    @Override
    protected void rollbackPack(Collection<DfsPackDescription> desc) {
        for (DfsPackDescription pack : desc) {
            {
                ObjectStorePath swiftPath = getBlobPath((CloudDfsPackDescription) pack, PackExt.PACK);
                deleteQuietly(swiftPath);
            }
            {
                ObjectStorePath swiftPath = getBlobPath((CloudDfsPackDescription) pack, PackExt.INDEX);
                deleteQuietly(swiftPath);
            }
        }
    }

    private void deleteQuietly(ObjectStorePath swiftPath) {
        try {
            swiftPath.delete();
        } catch (IOException e) {
            log.warn("Error deleting file {}", swiftPath);
        }
    }

    @Override
    protected ReadableChannel openFile(DfsPackDescription desc, PackExt ext) throws FileNotFoundException, IOException {
        CloudDfsPackDescription swiftPack = (CloudDfsPackDescription) desc;

        ObjectStorePath blobPath = getBlobPath(swiftPack, ext);

        ObjectInfo info = blobPath.getInfo();
        if (info == null) {
            throw new FileNotFoundException(desc.getFileName(ext));
        }
        return new BlobReadableChannel(blobPath, info.getLength());
    }

    ObjectStorePath getBlobPath(CloudDfsPackDescription swiftPack, PackExt ext) {
        ObjectStorePath swiftPath = blobPath.child(swiftPack.getFileName(ext));
        return swiftPath;
    }

    @Override
    protected DfsOutputStream writeFile(DfsPackDescription desc, final PackExt ext) throws IOException {
        CloudDfsPackDescription swiftPack = (CloudDfsPackDescription) desc;

        ObjectStorePath blobPath = getBlobPath(swiftPack, ext);

        File temp = File.createTempFile("tmp", ext.getExtension(), tempDir);

        return new BlobOutputStream(blobPath, temp);
    }

}
