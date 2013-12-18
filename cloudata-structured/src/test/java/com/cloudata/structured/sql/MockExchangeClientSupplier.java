package com.cloudata.structured.sql;

import com.facebook.presto.operator.ExchangeClient;
import com.google.common.base.Supplier;

public class MockExchangeClientSupplier implements Supplier<ExchangeClient> {
    @Override
    public ExchangeClient get() {
        throw new UnsupportedOperationException();
    }
}
