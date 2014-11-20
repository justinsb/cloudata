package com.cloudata.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;

public abstract class ThreadService extends AbstractService {

    private static final Logger log = LoggerFactory.getLogger(ThreadService.class);

    Thread thread;

    @Override
    protected void doStart() {
        try {
            Preconditions.checkState(thread == null);
            thread = new Thread(new Runner());
            thread.start();
            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        try {
            assert this.state() != State.RUNNING;
            assert this.state() != State.STARTING;

            if (thread != null) {
                thread.join();
                thread = null;
            }

            // notifyStopped();
            assert this.state() == State.TERMINATED;
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    protected abstract void run0();

    class Runner implements Runnable {
        @Override
        public void run() {
            try {
                run0();
            } catch (Throwable t) {
                log.error("Unexpected error during thread cycle", t);
                // And we exit the thread...
            }

            notifyStopped();
        }
    }

    protected boolean shouldStop() {
        State state = this.state();
        switch (state) {
        case STOPPING:
        case TERMINATED:
            return true;

        default:
            return false;
        }
    }

}
