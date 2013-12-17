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
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;

import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.operation.DeleteOperation;
import com.cloudata.keyvalue.operation.IncrementOperation;
import com.cloudata.keyvalue.operation.SetOperation;
import com.cloudata.keyvalue.KeyValueStateMachine;
import com.cloudata.values.Value;
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

        Value v = stateMachine.get(storeId, getKeyspace(), ByteString.copyFrom(k));

        if (v == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        ByteBuffer data = v.asBytes();
        return Response.ok(data).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response query() throws IOException {
        BtreeQuery query = stateMachine.scan(storeId);

        query.setFormat(MediaType.APPLICATION_OCTET_STREAM_TYPE);
        return Response.ok(query).build();
    }

    enum PostAction {
        SET, INCREMENT
    }

    @POST
    @Path("{key}")
    // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response post(@PathParam("key") String key, @QueryParam("action") String actionString,
            InputStream valueStream) throws IOException {
        try {
            ByteString k = ByteString.copyFrom(BaseEncoding.base16().decode(key));
            byte[] v = ByteStreams.toByteArray(valueStream);

            KvAction action = KvAction.SET;

            if (actionString != null) {
                actionString = actionString.toUpperCase();
                action = KvAction.valueOf(actionString);
            }

            Object ret;

            switch (action) {
            case SET: {
                Value value = Value.fromRawBytes(v);
                SetOperation operation = new SetOperation(value);
                stateMachine.doAction(storeId, getKeyspace(), k, operation);
                ret = null;
                break;
            }

            case INCREMENT: {
                IncrementOperation operation = new IncrementOperation(1);
                Long newValue = stateMachine.doAction(storeId, getKeyspace(), k, operation);
                ret = Value.fromLong(newValue).asBytes();
                break;
            }

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

            stateMachine.doAction(storeId, getKeyspace(), ByteString.copyFrom(k), new DeleteOperation());

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

    private Keyspace getKeyspace() {
        return Keyspace.ZERO;
    }

}
