package com.cloudata.keyvalue.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.Headers;
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry;
import com.cloudata.keyvalue.KeyValueStateMachine;
import com.cloudata.keyvalue.operation.DeleteOperation;
import com.cloudata.keyvalue.operation.IncrementOperation;
import com.cloudata.keyvalue.operation.SetOperation;
import com.cloudata.util.InetSocketAddresses;
import com.cloudata.values.Value;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;

@Path("/{storeId}/")
public class KeyValueEndpoint {

  private static final Logger log = LoggerFactory.getLogger(KeyValueEndpoint.class);

  @Inject
  KeyValueStateMachine stateMachine;

  @PathParam("storeId")
  long storeId;

  @Context
  HttpHeaders httpHeaders;

  @GET
  @Path("{key}")
  // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response get(@PathParam("key") String keyString) throws IOException, InterruptedException, RaftException {
    ByteString key = ByteString.copyFrom(BaseEncoding.base16().decode(keyString));

    Value value = stateMachine.get(storeId, getKeyspace(), key);

    ResponseBuilder response;
    if (value == null) {
      response = Response.status(Status.NOT_FOUND);
    } else {
      response = Response.ok(value.asBytes());
    }

    addClusterMap(response);

    return response.build();
  }

  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response query() throws IOException {
    BtreeQuery query = stateMachine.scan(storeId, getKeyspace(), null);

    query.setFormat(MediaType.APPLICATION_OCTET_STREAM_TYPE);
    ResponseBuilder response = Response.ok(query);
    addClusterMap(response);
    return response.build();
  }

  enum PostAction {
    SET, INCREMENT
  }

  @POST
  @Path("{key}")
  // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response post(@PathParam("key") String keyString, @QueryParam("action") String actionString,
      InputStream valueStream) throws IOException {
    ResponseBuilder response;
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
        ActionResponse actionResponse = stateMachine.doActionSync(operation);
        log.debug("SET gave result: {}", actionResponse);
        ret = null;
        break;
      }

      case INCREMENT: {
        IncrementOperation operation = IncrementOperation.build(storeId, keyspace, key, 1);
        ActionResponse actionResponse = stateMachine.doActionSync(operation);
        if (actionResponse.getEntryCount() != 0) {
          ResponseEntry entry = actionResponse.getEntry(0);
          ret = entry.getValue();
        } else {
          ret = null;
        }
        break;
      }

      default:
        throw new IllegalArgumentException();
      }

      response = Response.ok(ret);
    } catch (InterruptedException e) {
      log.warn("Error from Raft", e);
      response = Response.status(Response.Status.SERVICE_UNAVAILABLE);
    } catch (NotLeaderException e) {
      log.warn("Error from Raft", e);
      Optional<Replica> leader = e.getLeader();
      if (leader.isPresent()) {
        URI uri = replicaToUri(leader.get());
        log.debug("Redirecting to leader: {}", uri);
        response = Response.seeOther(uri);
      } else {
        response = Response.status(Response.Status.SERVICE_UNAVAILABLE);
      }
    } catch (RaftException e) {
      log.warn("Error from Raft", e);
      response = Response.serverError();
    }

    addClusterMap(response);

    return response.build();
  }

  private URI replicaToUri(Replica leader) {
    InetSocketAddress address = (InetSocketAddress) leader.address();
    int id = address.getPort() - 10000;
    int httpPort = 9990 + id;
    URI uri = URI.create("http://" + address.getHostName() + ":" + httpPort/* + "/" + key */);
    return uri;
  }


  private URI serverToUri(String replica) {
    InetSocketAddress address = InetSocketAddresses.parse(replica);
    int id = address.getPort() - 10000;
    int httpPort = 9990 + id;
    URI uri = URI.create("http://" + address.getHostName() + ":" + httpPort/* + "/" + key */);
    return uri;
  }

  @DELETE
  @Path("{key}")
  public Response delete(@PathParam("key") String keyString) throws IOException {
    ResponseBuilder response;

    try {
      ByteString key = ByteString.copyFrom(BaseEncoding.base16().decode(keyString));

      Keyspace keyspace = getKeyspace();

      stateMachine.doActionSync(DeleteOperation.build(storeId, keyspace, key));

      response = Response.noContent();
    } catch (InterruptedException e) {
      response = Response.status(Response.Status.SERVICE_UNAVAILABLE);
    } catch (NotLeaderException e) {
      Optional<Replica> leader = e.getLeader();
      if (leader.isPresent()) {
        URI uri = replicaToUri(leader.get());
        log.debug("Redirecting to leader: {}", uri);
        response = Response.seeOther(uri);
      } else {
        response = Response.status(Response.Status.SERVICE_UNAVAILABLE);
      }
    } catch (RaftException e) {
      response = Response.serverError();
    }

    addClusterMap(response);

    return response.build();
  }

  private void addClusterMap(ResponseBuilder response) {
    List<String> versionHeader = httpHeaders.getRequestHeader(Headers.CLUSTER_VERSION);
    String clusterVersion = versionHeader != null ? Iterables.getFirst(versionHeader, null) : null;
    if (clusterVersion != null) {
      RaftMembership raftMembership = stateMachine.getRaftMembership();
      long id = raftMembership.getId();
      if (!Long.toString(id).equals(clusterVersion)) {
        response.header(Headers.CLUSTER_VERSION, Long.toString(id));
        response.header(Headers.CLUSTER_MEMBERS,
            Joiner.on(",").join(Iterables.transform(raftMembership.getMembers(), new Function<String, String>() {
              @Override
              public String apply(String member) {
                return serverToUri(member).toString();
              }
            })));

        Optional<String> leader = stateMachine.getLeader();
        if (leader.isPresent()) {
          response.header(Headers.CLUSTER_LEADER, serverToUri(leader.get()).toString());
        }
      }
    }
  }

  private Keyspace getKeyspace() {
    return Keyspace.ZERO;
  }

}
