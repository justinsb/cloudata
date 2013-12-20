package com.cloudata.files;

import java.io.IOException;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.fs.FsClient;
import com.cloudata.files.fs.FsCredentials;
import com.cloudata.files.fs.FsVolume;
import com.cloudata.files.webdav.netty.WebdavNettyServer;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class FileServer {
    private static final Logger log = LoggerFactory.getLogger(FileServer.class);

    int httpPort = 8888;

    @Inject
    WebdavNettyServer nettyServer;

    @Inject
    FsClient fsClient;

    public synchronized void start() throws Exception {
        Preconditions.checkState(!nettyServer.isStarted());
        nettyServer.start(httpPort);
    }

    public static void main(String... args) throws Exception {
        Injector injector = Guice.createInjector(new FileServerModule());

        final FileServer server = injector.getInstance(FileServer.class);

        FsCredentials credentials = null;
        server.ensureFilesystem(FsVolume.fromHost("1"), credentials);

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (Exception e) {
                    log.warn("Error shutting down HTTP server", e);
                }
            }
        });
    }

    private void ensureFilesystem(FsVolume volume, FsCredentials credentials) throws IOException {
        fsClient.ensureFilesystem(volume, credentials);
    }

    public synchronized void stop() throws Exception {
        if (nettyServer != null) {
            nettyServer.stop();
            nettyServer = null;
        }
    }

}
