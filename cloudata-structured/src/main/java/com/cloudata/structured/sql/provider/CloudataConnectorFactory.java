/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudata.structured.sql.provider;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import com.cloudata.structured.StructuredStore;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class CloudataConnectorFactory implements ConnectorFactory {
    public static final String PROVIDER_ID = "cloudata";

    private final Map<String, String> optionalConfig;
    private final NodeManager nodeManager;

    private final Map<String, CloudataConnector> connectors = Maps.newHashMap();

    private final StructuredStore store;

    public CloudataConnectorFactory(NodeManager nodeManager, Map<String, String> optionalConfig, StructuredStore store) {
        this.nodeManager = nodeManager;
        this.store = store;
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public String getName() {
        return PROVIDER_ID;
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig) {
        checkNotNull(requiredConfig, "requiredConfig is null");
        checkNotNull(optionalConfig, "optionalConfig is null");

        try {
            // // A plugin is not required to use Guice; it is just very convenient
            // Bootstrap app = new Bootstrap(new JsonModule(), new ExampleModule(connectorId));
            //
            // Injector injector = app.strictConfig().doNotInitializeLogging()
            // .setRequiredConfigurationProperties(requiredConfig)
            // .setOptionalConfigurationProperties(optionalConfig).initialize();

            ClassToInstanceMap<Object> services = ImmutableClassToInstanceMap.builder()

            .put(ConnectorMetadata.class, new CloudataConnectorMetadata(connectorId, store))

            .put(ConnectorSplitManager.class, new CloudataSplitManager(nodeManager, connectorId))

            .put(ConnectorRecordSetProvider.class, new CloudataConnectorRecordSetProvider())

            .put(ConnectorHandleResolver.class, new CloudataConnectorHandleResolver()).build();

            CloudataConnector connector = new CloudataConnector(store, services);
            connectors.put(connectorId, connector);
            return connector;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public CloudataConnector get(String connectorId) {
        return connectors.get(connectorId);
    }
}
