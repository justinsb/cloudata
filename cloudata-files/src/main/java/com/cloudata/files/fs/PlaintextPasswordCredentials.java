package com.cloudata.files.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaintextPasswordCredentials implements FsCredentials {
    private static final Logger log = LoggerFactory.getLogger(PlaintextPasswordCredentials.class);

    private final String username;
    private final String password;

    public PlaintextPasswordCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

}
