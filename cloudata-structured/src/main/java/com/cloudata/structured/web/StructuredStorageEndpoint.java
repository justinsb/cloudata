package com.cloudata.structured.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;

import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Keyspace;
import com.cloudata.structured.Listener;
import com.cloudata.structured.StructuredStateMachine;
import com.cloudata.structured.StructuredStore;
import com.cloudata.structured.operation.StructuredDeleteOperation;
import com.cloudata.structured.operation.StructuredSetOperation;
import com.cloudata.values.Value;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

@Path("/{storeId}/")
public class StructuredStorageEndpoint {

    @Inject
    StructuredStateMachine stateMachine;

    @PathParam("storeId")
    long storeId;

    StructuredStore store;

    StructuredStore getStore() {
        if (store == null) {
            store = stateMachine.getStructuredStore(storeId);
        }
        return store;
    }

    @GET
    @Path("{keyspace}/{key}")
    // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response get(@PathParam("keyspace") String keyspaceString, @PathParam("key") String keyString)
            throws IOException {
        ByteString key = ByteString.copyFromUtf8(keyString);

        StructuredStore store = getStore();

        Keyspace keyspace = store.getKeyspaces().findKeyspace(keyspaceString);
        if (keyspace == null) {
            throw new WebApplicationException(Status.NOT_FOUND);
        }

        Value v = stateMachine.get(store, keyspace, key);

        if (v == null) {
            throw new WebApplicationException(Status.NOT_FOUND);
        }

        ByteBuffer data = v.asBytes();
        return Response.ok(data).build();
    }

    @GET
    @Path("{keyspace}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response queryJson(@PathParam("keyspace") String keyspaceString) throws IOException {
        StructuredStore store = getStore();

        Keyspace keyspace = store.getKeyspaces().findKeyspace(keyspaceString);
        if (keyspace == null) {
            throw new WebApplicationException(Status.NOT_FOUND);
        }

        BtreeQuery query = stateMachine.scan(store, keyspace);
        if (query == null) {
            throw new WebApplicationException(Status.NOT_FOUND);
        }

        query.setFormat(MediaType.APPLICATION_JSON_TYPE);
        return Response.ok(query).build();
    }

    @GET
    @Path("keys/{keyspace}")
    public Response listKeys(@PathParam("keyspace") String keyspaceString) throws IOException {
        final StructuredStore store = getStore();

        final Keyspace keyspace = store.getKeyspaces().findKeyspace(keyspaceString);
        if (keyspace == null) {
            throw new WebApplicationException(Status.NOT_FOUND);
        }

        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                final CodedOutputStream cos = CodedOutputStream.newInstance(os);
                final byte[] buffer = new byte[8192];

                stateMachine.listKeys(store, keyspace, new Listener<ByteBuffer>() {

                    @Override
                    public boolean next(ByteBuffer value) {
                        try {
                            value = value.duplicate();

                            cos.writeInt32NoTag(value.remaining());

                            while (true) {
                                int n = Math.min(value.remaining(), buffer.length);
                                if (n == 0) {
                                    break;
                                }

                                value.get(buffer, 0, n);
                                cos.writeRawBytes(buffer, 0, n);
                            }

                            return true;
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }

                    @Override
                    public void done() {
                        try {
                            cos.writeInt32NoTag(-1);
                            cos.flush();
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });

            }
        };

        return Response.ok(stream).build();
    }

    enum PostAction {
        SET, INCREMENT
    }

    @POST
    @Path("{keyspace}/{key}")
    // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response post(@PathParam("keyspace") String keyspaceString, @PathParam("key") String keyString,
            InputStream valueStream) throws IOException {
        try {
            ByteString key = ByteString.copyFrom(keyString.getBytes(Charsets.UTF_8));
            byte[] v = ByteStreams.toByteArray(valueStream);

            Object ret;

            Value value = Value.fromJsonBytes(v);

            StructuredStore store = getStore();

            Keyspace keyspace = store.getKeyspaces().findKeyspace(keyspaceString);
            if (keyspace == null) {
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            StructuredSetOperation operation = StructuredSetOperation.build(storeId, keyspace, key, value);
            stateMachine.doActionSync(operation);
            ret = null;

            return Response.ok(ret).build();
        } catch (InterruptedException e) {
            throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
        } catch (NoLeaderException e) {
            throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
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
    @Path("{keyspace}/{key}")
    public Response delete(@PathParam("keyspace") String keyspaceString, @PathParam("key") String keyString)
            throws IOException {
        try {
            ByteString key = ByteString.copyFrom(keyString.getBytes(Charsets.UTF_8));

            StructuredStore store = getStore();

            Keyspace keyspace = store.getKeyspaces().findKeyspace(keyspaceString);
            if (keyspace == null) {
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            stateMachine.doActionSync(StructuredDeleteOperation.build(storeId, keyspace, key));

            return Response.noContent().build();
        } catch (InterruptedException e) {
            throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
        } catch (NoLeaderException e) {
            throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
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
