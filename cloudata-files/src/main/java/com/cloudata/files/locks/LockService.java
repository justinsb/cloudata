package com.cloudata.files.locks;

public interface LockService {
    CloudLock get(String key);

    // CloudLockToken lock(String key);
    //
    // CloudLockToken unlock(String key, String lockToken);
    //
    // CloudLockToken findLock(String key, String lockToken);

}
