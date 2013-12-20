package com.cloudata.files.locks;

import java.util.concurrent.locks.Lock;

public interface CloudLock extends Lock {
    CloudLockToken getLockToken();
}
