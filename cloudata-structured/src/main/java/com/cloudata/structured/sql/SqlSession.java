package com.cloudata.structured.sql;

import com.facebook.presto.sql.analyzer.Session;

public class SqlSession {

    final Session prestoSession;

    public SqlSession() {
        String user = "user";
        String source = "source";
        String catalog = "default";
        String schema = "default";
        String remoteUserAddress = "remoteUserAddress";
        String userAgent = "userAgent";
        this.prestoSession = new Session(user, source, catalog, schema, remoteUserAddress, userAgent);
    }
}
