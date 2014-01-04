package com.cloudata.blockstore.backend.cloud;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.blockstore.VolumeProto.VolumeData;
import com.cloudata.blockstore.VolumeProvider;
import com.cloudata.clients.keyvalue.IfNotExists;
import com.cloudata.clients.keyvalue.KeyValueService;
import com.cloudata.clients.keyvalue.KeyValueStore;
import com.cloudata.files.blobs.BlobService;
import com.cloudata.files.blobs.BlobStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

public class CloudVolumeProvider implements VolumeProvider {
    private static final Logger log = LoggerFactory.getLogger(CloudVolumeProvider.class);

    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024;

    private static final int SPACE_NAMES = 0;

    final LoadingCache<ByteString, CloudVolume> volumeCache;
    final ThreadPools executors;

    final KeyValueStore namesStore;

    final KeyValueService keyValueService;
    final BlobService blobService;

    @Inject
    public CloudVolumeProvider(ThreadPools executors, KeyValueService keyValueService, BlobService blobService) {
        this.executors = executors;

        this.namesStore = keyValueService.get(ServiceIds.CLOUD_VOLUMES);

        this.keyValueService = keyValueService;
        this.blobService = blobService;

        VolumeCacheLoader loader = new VolumeCacheLoader();
        this.volumeCache = CacheBuilder.newBuilder().recordStats().build(loader);
    }

    @Override
    public CloudVolume get(ByteString id) {
        try {
            return volumeCache.get(id);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public CloudVolume create(ByteString name, long sizeBytes) throws IOException {
        Preconditions.checkArgument(sizeBytes > 0);
        Preconditions.checkArgument(name.size() > 0);

        if (namesStore.read(SPACE_NAMES, name) != null) {
            throw new FileAlreadyExistsException(null);
        }

        ByteString blobStoreId = null;
        ByteString segmentStoreId = null;
        try {
            blobStoreId = blobService.allocate();
            segmentStoreId = keyValueService.allocate();

            VolumeData volume;
            {
                VolumeData.Builder b = VolumeData.newBuilder();
                b.setBlobStoreId(blobStoreId);
                b.setSegmentStoreId(segmentStoreId);

                int segmentSize = DEFAULT_SEGMENT_SIZE;
                long segments = (sizeBytes + (segmentSize - 1)) / segmentSize;
                b.setSegmentCount(Ints.checkedCast(segments));
                b.setSegmentSize(segmentSize);

                volume = b.build();
            }

            ByteString value = volume.toByteString();

            if (!namesStore.putSync(SPACE_NAMES, name, value, IfNotExists.INSTANCE)) {
                throw new FileAlreadyExistsException(null);
            }

            // Don't release
            blobStoreId = null;
            segmentStoreId = null;

            return get(name);
        } finally {
            if (blobStoreId != null) {
                blobService.delete(blobStoreId);
            }
            if (segmentStoreId != null) {
                keyValueService.delete(segmentStoreId);
            }
        }

    }

    @Immutable
    final class VolumeCacheLoader extends CacheLoader<ByteString, CloudVolume> {

        @Override
        public CloudVolume load(@Nonnull ByteString volumeName) throws Exception {
            assert volumeName != null;

            try {
                ByteString volumeValue = namesStore.read(SPACE_NAMES, volumeName);
                if (volumeValue == null) {
                    throw new FileNotFoundException();
                }

                VolumeData volumeData = VolumeData.parseFrom(volumeValue);

                KeyValueStore segmentsKeyValueStore = keyValueService.get(volumeData.getSegmentStoreId());
                BlobStore blobStore = blobService.get(volumeData.getBlobStoreId());

                return new CloudVolume(executors, segmentsKeyValueStore, blobStore, volumeData);
            } catch (Exception e) {
                log.warn("Error building CloudVolume", e);
                throw e;
            }
        }
    }

    @Override
    public CloudVolume ensureVolume(ByteString name, int sizeGb) throws IOException {
        long sizeBytes = sizeGb;
        sizeBytes *= 1024;
        sizeBytes *= 1024;
        sizeBytes *= 1024;

        try {
            return create(name, sizeBytes);
        } catch (FileAlreadyExistsException e) {
            return get(name);
        }
    }

}
