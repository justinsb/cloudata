package com.cloudata.blockstore;

import io.netty.util.ResourceLeakDetector;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.blockstore.iscsi.IscsiEndpoint;
import com.cloudata.blockstore.iscsi.IscsiServer;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

public class BlockStoreServer {
    private static final Logger log = LoggerFactory.getLogger(BlockStoreServer.class);

    private final SocketAddress iscsiSocketAddress;

    private IscsiServer iscsiServer;
    final File basePath;

    public BlockStoreServer(SocketAddress iscsiSocketAddress, File basePath) {
        this.basePath = basePath;
        this.iscsiSocketAddress = iscsiSocketAddress;
    }

    public synchronized void start() throws Exception {
        InetSocketAddress redisSocketAddress = new InetSocketAddress("localhost", 6379);
        // Injector injector = Guice.createInjector(new BlockStoreModule(), new WebModule());
        Injector injector = Guice.createInjector(new BlockStoreModule(basePath, redisSocketAddress));

        if (iscsiSocketAddress != null) {
            this.iscsiServer = injector.getInstance(IscsiServer.class);

            IscsiEndpoint iscsiEndpoint = iscsiServer.createEndpoint(iscsiSocketAddress);
            iscsiEndpoint.start();

            {
                long lun = 0;
                ByteString name = IscsiServer.buildName(lun);
                iscsiServer.ensureVolume(name, 20);
            }
        }
    }

    public static void main(String... args) throws Exception {
        ResourceLeakDetector.setEnabled(true);

        final int port = Integer.parseInt(args[0]);

        int iscsiPort = 3260 + port;

        SocketAddress iscsiSocketAddress = new InetSocketAddress(iscsiPort);
        File basePath = new File("data" + port);
        final BlockStoreServer server = new BlockStoreServer(iscsiSocketAddress, basePath);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (Exception e) {
                    log.error("Error stopping server", e);
                }
            }
        });
    }

    public synchronized void stop() throws Exception {
        if (iscsiServer != null) {
            try {
                iscsiServer.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            iscsiServer = null;
        }

    }

    public SocketAddress getIscsiSocketAddress() {
        return iscsiSocketAddress;
    }

}
