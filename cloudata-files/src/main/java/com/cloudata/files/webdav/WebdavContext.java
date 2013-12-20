package com.cloudata.files.webdav;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.cloudata.files.fs.FsClient;
import com.cloudata.files.fs.FsContext;
import com.cloudata.files.locks.LockService;

@Singleton
public class WebdavContext {
    @Inject
    MimeHelper mimeHelper;

    @Inject
    FsContext syncActions;

    @Inject
    FsClient fsClient;

    @Inject
    LockService lockService;
}
