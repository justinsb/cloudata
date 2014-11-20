package com.cloudata.services;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;

public abstract class CompoundService extends AbstractService {

  private static final Logger log = LoggerFactory.getLogger(JettyService.class);

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
              log.debug("Stopping service {}", service);
                service.stopAsync().awaitTerminated();
            }
            log.debug("Stopped all services");
            
            notifyStopped();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

}
