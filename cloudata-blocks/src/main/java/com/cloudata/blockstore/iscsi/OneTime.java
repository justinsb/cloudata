package com.cloudata.blockstore.iscsi;

import java.util.concurrent.atomic.AtomicInteger;

public class OneTime {
    final AtomicInteger count = new AtomicInteger();

    public boolean isFirst() {
        return count.incrementAndGet() == 1;
    }

}
