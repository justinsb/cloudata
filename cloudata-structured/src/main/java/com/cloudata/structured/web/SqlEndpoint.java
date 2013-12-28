package com.cloudata.structured.web;

import java.io.IOException;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudata.structured.sql.SqlEngine;
import com.cloudata.structured.sql.SqlEngineFactory;
import com.cloudata.structured.sql.SqlSession;
import com.cloudata.structured.sql.SqlStatement;

@Path("/{storeId}/sql")
public class SqlEndpoint {

    @Inject
    SqlEngineFactory sqlEngineFactory;

    @PathParam("storeId")
    long storeId;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response queryJson(@QueryParam("sql") String sql) throws IOException {
        SqlEngine sqlEngine = sqlEngineFactory.get(storeId);

        SqlSession session = sqlEngine.createSession();

        SqlStatement statement = sqlEngine.parse(session, sql);
        if (!statement.isSimple()) {
            throw new UnsupportedOperationException();
        }

        return Response.ok(statement).build();
    }

}
