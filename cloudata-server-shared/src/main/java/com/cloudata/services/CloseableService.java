package com.cloudata.services;

import java.io.Closeable;

import com.google.common.util.concurrent.AbstractService;

public class CloseableService extends AbstractService {
    private final Closeable closeable;

    public CloseableService(Closeable closeable) {
        this.closeable = closeable;
    }

    @Override
    protected void doStart() {
        try {
//            closeable.init(raft);
            this.notifyStarted();
        } catch (Exception e) {
            this.notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        try {
            if (closeable != null) {
              closeable.close();
            }
            this.notifyStopped();
        } catch (Exception e) {
            this.notifyFailed(e);
        }
    }
}
