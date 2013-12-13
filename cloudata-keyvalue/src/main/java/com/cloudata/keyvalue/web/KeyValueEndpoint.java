package com.cloudata.keyvalue.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.cloudata.keyvalue.KeyValueStateMachine;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;

@Path("/{storeId}/")
public class KeyValueEndpoint {

    @Inject
    KeyValueStateMachine stateMachine;

    @PathParam("storeId")
    long storeId;

    @GET
    @Path("{key}")
    // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response get(@PathParam("key") String key) throws IOException {
        byte[] k = BaseEncoding.base16().decode(key);

        ByteBuffer v = stateMachine.get(storeId, ByteBuffer.wrap(k));

        if (v == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        return Response.ok(v).build();
    }

    enum PostAction {
        SET, INCREMENT
    }

    @POST
    @Path("{key}")
    // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response post(@PathParam("key") String key, @QueryParam("action") String actionString, InputStream value)
            throws IOException {
        try {
            byte[] k = BaseEncoding.base16().decode(key);
            byte[] v = ByteStreams.toByteArray(value);

            KvAction action = KvAction.SET;

            if (actionString != null) {
                actionString = actionString.toUpperCase();
                action = KvAction.valueOf(actionString);
            }

            Object ret = null;

            KvEntry.Builder entry = KvEntry.newBuilder().setStoreId(storeId).setKey(ByteString.copyFrom(k))
                    .setAction(action).setValue(ByteString.copyFrom(v));

            switch (action) {
            case SET:
            case INCREMENT:
                ret = stateMachine.doAction(entry.build());
                break;

            default:
                throw new IllegalArgumentException();
            }

            return Response.ok(ret).build();
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

    @DELETE
    @Path("{key}")
    public Response delete(@PathParam("key") String key) throws IOException {
        try {
            byte[] k = BaseEncoding.base16().decode(key);

            KvEntry entry = KvEntry.newBuilder().setStoreId(storeId).setKey(ByteString.copyFrom(k))
                    .setAction(KvAction.DELETE).build();

            stateMachine.doAction(entry);

            return Response.noContent().build();
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
