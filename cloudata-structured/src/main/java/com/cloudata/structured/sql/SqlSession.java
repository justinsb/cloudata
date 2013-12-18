package com.cloudata.structured.sql;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Session;

public class SqlSession {

    final SqlEngine sqlEngine;
    final Session prestoSession;

    SqlSession(SqlEngine sqlEngine, String catalog) {
        this.sqlEngine = sqlEngine;
        String user = "user";
        String source = "source";
        String schema = "default";
        String remoteUserAddress = "remoteUserAddress";
        String userAgent = "userAgent";
        this.prestoSession = new Session(user, source, catalog, schema, remoteUserAddress, userAgent);
    }

    void execute(SqlStatement sqlStatement, final RowsetListener listener) {
        sqlEngine.execute(this, sqlStatement, listener);
    }

    Metadata getMetadata() {
        return sqlEngine.getMetadata();
    }
}
