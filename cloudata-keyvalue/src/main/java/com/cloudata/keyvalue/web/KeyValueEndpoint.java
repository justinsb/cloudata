package com.cloudata.keyvalue.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;

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
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry;
import com.cloudata.keyvalue.KeyValueStateMachine;
import com.cloudata.keyvalue.operation.DeleteOperation;
import com.cloudata.keyvalue.operation.IncrementOperation;
import com.cloudata.keyvalue.operation.SetOperation;
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
    public Response get(@PathParam("key") String keyString) throws IOException, InterruptedException, RaftException {
        ByteString key = ByteString.copyFrom(BaseEncoding.base16().decode(keyString));

        Value value = stateMachine.get(storeId, getKeyspace(), key);

        if (value == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        return Response.ok(value.asBytes()).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response query() throws IOException {
        BtreeQuery query = stateMachine.scan(storeId, getKeyspace(), null);

        query.setFormat(MediaType.APPLICATION_OCTET_STREAM_TYPE);
        return Response.ok(query).build();
    }

    enum PostAction {
        SET, INCREMENT
    }

    @POST
    @Path("{key}")
    // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response post(@PathParam("key") String keyString, @QueryParam("action") String actionString,
            InputStream valueStream) throws IOException {
        try {
            ByteString key = ByteString.copyFrom(BaseEncoding.base16().decode(keyString));
            byte[] v = ByteStreams.toByteArray(valueStream);

            Keyspace keyspace = getKeyspace();
            ActionType action = ActionType.SET;

            if (actionString != null) {
                actionString = actionString.toUpperCase();
                action = ActionType.valueOf(actionString);
            }

            Object ret;

            switch (action) {
            case SET: {
                Value value = Value.fromRawBytes(v);
                SetOperation operation = SetOperation.build(storeId, keyspace, key, value);
                stateMachine.doActionSync(operation);
                ret = null;
                break;
            }

            case INCREMENT: {
                IncrementOperation operation = IncrementOperation.build(storeId, keyspace, key, 1);
                ActionResponse response = stateMachine.doActionSync(operation);
                if (response.getEntryCount() != 0) {
                    ResponseEntry entry = response.getEntry(0);
                    ret = Value.deserialize(entry.getValue()).asBytes();
                } else {
                    ret = null;
                }
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
    public Response delete(@PathParam("key") String keyString) throws IOException {
        try {
            ByteString key = ByteString.copyFrom(BaseEncoding.base16().decode(keyString));

            Keyspace keyspace = getKeyspace();

            stateMachine.doActionSync(DeleteOperation.build(storeId, keyspace, key));

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
