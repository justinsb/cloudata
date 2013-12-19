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

import java.util.Map;

import com.cloudata.structured.StructuredStore;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;

public class CloudataConnector implements Connector {
    private final ClassToInstanceMap<Object> services;
    private final StructuredStore store;

    public CloudataConnector(StructuredStore store, Map<Class<?>, ?> services) {
        this.store = store;
        this.services = ImmutableClassToInstanceMap.copyOf(services);
    }

    @Override
    public <T> T getService(Class<T> type) {
        return services.getInstance(type);
    }

    public RecordSet getRecordset(TableMetadata tableMetadata) {
        return new CloudataRecordSet(store, tableMetadata);
    }
}