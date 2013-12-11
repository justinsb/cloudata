package com.cloudata.appendlog.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Iterator;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;

import com.cloudata.appendlog.AppendLogStore;
import com.cloudata.appendlog.log.LogMessage;
import com.cloudata.appendlog.log.LogMessageIterable;
import com.google.common.io.ByteStreams;

@Path("/{logid}/")
public class AppendLogEndpoint {

    public static final String HEADER_CRC = "X-Crc";

    public static final String HEADER_NEXT = "X-Next";

    @Inject
    AppendLogStore database;

    @PathParam("logid")
    long logId;

    @GET
    @Path("{position}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response get(@PathParam("position") String position) {
        long begin = Long.valueOf(position, 16);
        int max = 1;
        LogMessageIterable messages = database.readLog(logId, begin, max);
        Iterator<LogMessage> it = messages.iterator();
        if (it.hasNext()) {
            LogMessage message = it.next();
            ByteBuffer payload = message.getValue();
            return Response.ok(payload).header(HEADER_CRC, Long.toHexString(message.getCrc()))
                    .header(HEADER_NEXT, Long.toHexString(begin + message.size())).build();
        }

        return Response.status(Status.NOT_FOUND).build();
    }

    @POST
    public Response post(InputStream value) throws IOException {
        try {
            byte[] v = ByteStreams.toByteArray(value);

            database.appendToLog(logId, v);

            return Response.ok().build();
        } catch (InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        } catch (NoLeaderException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        } catch (NotLeaderException e) {
            Replica leader = e.getLeader();
            InetSocketAddress address = (InetSocketAddress) leader.address();
            URI uri = URI.create("http://" + address.getHostName() + ":" + address.getPort() /* + "/" + key */);
            System.out.println(uri);
            return Response.seeOther(uri).build();
        } catch (RaftException e) {
            return Response.serverError().build();
        }
    }

}
