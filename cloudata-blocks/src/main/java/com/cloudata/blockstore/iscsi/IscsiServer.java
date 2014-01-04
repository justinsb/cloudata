package com.cloudata.blockstore.iscsi;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import com.cloudata.blockstore.Volume;
import com.cloudata.blockstore.VolumeProvider;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;

public class IscsiServer {
    short nextSessionIdentifier = 1;
    final Map<Long, Volume> volumes = Maps.newHashMap();
    final ListeningExecutorService executor;
    final List<IscsiEndpoint> endpoints = Lists.newArrayList();
    final VolumeProvider volumeProvider;

    @Inject
    public IscsiServer(VolumeProvider volumeProvider) {
        this.volumeProvider = volumeProvider;
        this.executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    }

    public synchronized short assignSessionIdentifier() {
        return nextSessionIdentifier++;
    }

    public Volume getVolume(long lun) {
        synchronized (volumes) {
            // TODO: Use Cache or similar
            Volume volume = volumes.get(lun);
            if (volume == null) {
                try {
                    // File f = new File("volumes/" + lun);
                    // if (!f.exists()) {
                    // f.getParentFile().mkdirs();
                    // try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                    // raf.setLength(100 * 1024 * 1024);
                    // }
                    // }
                    // volume = new MmapVolume(f, executor);

                    ByteString name = buildName(lun);

                    volume = volumeProvider.get(name);

                    volumes.put(lun, volume);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
            return volume;
        }
        // return new DummyVolume(lun);

    }

    public static ByteString buildName(long lun) {
        // TODO: Include the IQN?
        ByteBuffer name = ByteBuffer.allocate(256);
        name.putLong(lun);
        name.flip();

        return ByteString.copyFrom(name);

    }

    public void stop() throws InterruptedException {
        for (IscsiEndpoint endpoint : endpoints) {
            endpoint.stop();
        }
    }

    public IscsiEndpoint createEndpoint(SocketAddress iscsiSocketAddress) {
        IscsiEndpoint endpoint = new IscsiEndpoint(iscsiSocketAddress, this);
        endpoints.add(endpoint);
        return endpoint;
    }

    public void ensureVolume(ByteString name, int sizeGb) throws IOException {
        volumeProvider.ensureVolume(name, sizeGb);
    }

}
