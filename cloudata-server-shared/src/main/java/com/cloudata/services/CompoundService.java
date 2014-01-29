package com.cloudata.services;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;

public abstract class CompoundService extends AbstractService {

    List<Service> services;

    protected abstract List<Service> buildServices() throws Exception;

    protected synchronized List<Service> getServices() throws Exception {
        if (services == null) {
            services = buildServices();
        }
        return services;
    }

    @Override
    protected void doStart() {
        try {
            List<Service> services = getServices();

            for (Service service : services) {
                service.startAsync().awaitRunning();
            }

            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        try {
            List<Service> services = getServices();

            for (Service service : Lists.reverse(services)) {
                service.stopAsync().awaitTerminated();
            }

            notifyStopped();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

}
