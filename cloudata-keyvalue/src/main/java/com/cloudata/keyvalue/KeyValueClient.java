package com.cloudata.keyvalue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;

import org.robotninjas.barge.RaftMembership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.CloseableClientResponse;
import com.cloudata.TimeSpan;
import com.cloudata.clients.StreamingRecordsetBase;
import com.cloudata.util.ByteStringMessageBodyWriter;
import com.cloudata.util.Hex;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultiset;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.ClientFilter;

public class KeyValueClient {

  private static final Logger log = LoggerFactory.getLogger(KeyValueClient.class);

  static final Client CLIENT = buildClient();

  private ClusterState clusterState;

  public KeyValueClient(ClusterState clusterState) {
    this.clusterState = clusterState;
  }

  static class ClusterState {
    final PriorityMap<String> leaderScores = PriorityMap.create();

    public ClusterState(Collection<String> seeds) {
      for (String seed : seeds) {
        leaderScores.setPriority(seed, 0);
      }
    }

    public void recordLeaderHint(String fromServer, String leader) {
      setLeaderScore(fromServer, 0);
      addLeaderScore(leader, 1);
    }

    public void confirmedLeader(String leader) {
      addLeaderScore(leader, 1);
    }

    public void recordServerError(String server, String info) {
      setLeaderScore(server, -3);
    }

    private void setLeaderScore(String server, int newScore) {
      log.debug("Leader score server={} set={}", server, newScore);
      synchronized (leaderScores) {
        leaderScores.setPriority(server, newScore);
      }
    }

    private void addLeaderScore(String server, int delta) {
      log.debug("Leader score server={} delta={}", server, delta);

      synchronized (leaderScores) {
        leaderScores.addPriority(server, delta);
      }
    }

    public Iterable<String> keys() {
      synchronized (leaderScores) {
        return leaderScores.keys();
      }
    }

    public static ClusterState fromSeeds(String... seeds) {
      return new ClusterState(Arrays.asList(seeds));
    }

    public void notifyMembership(String fromServer, String leader, RaftMembership raftMembership) {
      if (!Objects.equal(fromServer, leader)) {
        setLeaderScore(fromServer, -1);
      }
      for (String member : raftMembership.getMembers()) {
        if (!Objects.equal(member, leader)) {
          setLeaderScore(member, 0);
        }
      }
      if (leader != null) {
        addLeaderScore(leader, 1);
      }
    }

    public String getLeader() {
      return leaderScores.getHighest();
    }

  };

  private static Client buildClient() {
    ClientConfig config = new DefaultClientConfig();
    config.getClasses().add(ByteStringMessageBodyWriter.class);
    Client client = Client.create(config);
    client.setFollowRedirects(true);
    client.addFilter(new ClientFilter() {

      @Override
      public ClientResponse handle(ClientRequest cr) throws ClientHandlerException {
        log.debug("Making request {} {}", cr.getMethod(), cr.getURI());

        ClientResponse response = getNext().handle(cr);

        return response;
      }

    });
    return client;
  }

  abstract class RunRequest<T> implements Callable<T> {

    boolean done = false;
    private T value;

    int attempt = 0;

    int maxFailPenalty = 5;
    // int maxSeedRetries = 3;
    // int maxLeaderRetries = 9;
    // int maxClusterRetries = 6;

    TimeSpan retryInternal = TimeSpan.millis(200);

    final String method;
    final String path;
    final ByteString postValue;

    public RunRequest(String method, String path) {
      this(method, path, null);
    }

    public RunRequest(String method, String path, ByteString postValue) {
      this.method = method;
      this.path = path;
      this.postValue = postValue;
    }

    protected WebResource addQueryParams(WebResource webResource) {
      return webResource;
    }

    protected void handleRedirect(String server, ClientResponse response) {
      URI location = response.getLocation();
      if (location == null) {
        throw new IllegalStateException("Unexpected redirect without location");
      }
      log.info("Following leader redirect from {} to {}", server, location);
      String leader = location.toString();
      clusterState.recordLeaderHint(server, leader);
    }

    // protected <E extends Exception> void maybeRetry(E e) throws E {
    // if (attempt < maxRetries) {
    // log.info("Will retry after exception: {}", e.getMessage());
    // this.retryInternal.sleep();
    // } else {
    // throw e;
    // }
    // }

    boolean closeResponse;
    private RaftMembership raftMembership;

    public T call() throws IOException {
      int maxAttempts = 2;
      for (int attempt = 0; attempt < maxAttempts - 1; attempt++) {
        try {
          return call0();
        } catch (IOException e) {
          log.warn("Error during query; will retry", e);
        }
      }
      
      return call0();
    }
    
    T call0() throws IOException {
      HashMultiset<String> serversQueried = HashMultiset.create();

      while (true) {
        String server = pickServer(serversQueried);
        if (server == null) {
          // No servers left to try
          throw new IOException("Service temporarily unavailable");
        }

        log.debug("Trying server {}", server);
        boolean goodResult = queryServer(server);

        if (done) {
          return value;
        }

        // We still don't want to infinitely retry redirects, but we penalize them less than an error
        int penalty = goodResult ? 1 : 3;

        serversQueried.add(server, penalty);
      }
    }

    private String pickServer(HashMultiset<String> serversQueried) {
      for (String key : clusterState.keys()) {
        if (serversQueried.count(key) < maxFailPenalty) {
          return key;
        }
      }

      return null;
    }

    protected boolean queryServer(String baseUrl) throws IOException {
      WebResource webResource = CLIENT.resource(baseUrl).path(path);
      webResource = addQueryParams(webResource);

      WebResource.Builder webResourceBuilder;
      if (postValue != null) {
        webResourceBuilder = webResource.entity(postValue, MediaType.APPLICATION_OCTET_STREAM_TYPE);
      } else {
        webResourceBuilder = webResource.getRequestBuilder();
      }

      if (raftMembership != null) {
        webResourceBuilder = webResourceBuilder.header(Headers.CLUSTER_VERSION, raftMembership.getId());
      } else {
        webResourceBuilder = webResourceBuilder.header(Headers.CLUSTER_VERSION, "any");
      }

      closeResponse = true;
      ClientResponse response = null;
      try {
        response = webResourceBuilder.method(method, ClientResponse.class);
        return processResponse(baseUrl, response);
      } catch (IOException e) {
        log.debug("Error querying server: {}", baseUrl, e);
        clusterState.recordServerError(baseUrl, null);
        return false;
      } catch (ClientHandlerException e) {
        log.debug("Error querying server: {}", baseUrl, e);
        clusterState.recordServerError(baseUrl, null);
        return false;
      } finally {
        if (response != null && closeResponse) {
          response.close();
        }
      }
    }

    protected boolean processResponse(String server, ClientResponse response) throws IOException {
      int status = response.getStatus();
      log.debug("Got response {} from {}", status, server);

      String clusterMembers = response.getHeaders().getFirst(Headers.CLUSTER_MEMBERS);
      if (clusterMembers != null) {
        long clusterId = Long.parseLong(response.getHeaders().getFirst(Headers.CLUSTER_VERSION));
        Iterable<String> members = Splitter.on(",").split(clusterMembers);
        this.raftMembership = new RaftMembership(clusterId, members);

        String leader = response.getHeaders().getFirst(Headers.CLUSTER_LEADER);
        // Leader might be null
        clusterState.notifyMembership(server, leader, this.raftMembership);
      }

      switch (status) {
      case 200:
      case 204:
      case 404:
        this.value = extractResult(response);
        this.done = true;
        return true;

      case 303:
        handleRedirect(server, response);
        return true;

      case 503:
        clusterState.recordServerError(server, null);
        return false;

      default:
        String info = getErrorInfo(response);
        clusterState.recordServerError(server, info);
        return false;
      }
    }

    protected abstract T extractResult(ClientResponse response) throws IOException;

  }

  public void put(final long storeId, final ByteString key, final ByteString value) throws Exception {
    RunRequest<Void> retry = new RunRequest<Void>(HttpMethod.POST, toUrlPath(storeId, key), value) {

      protected Void extractResult(ClientResponse response) {
        int status = response.getStatus();

        switch (status) {
        case 200:
          return null;

        default:
          throw new IllegalStateException("Unexpected status: " + status);
        }
      }
    };

    retry.call();
  }

  public KeyValueEntry increment(final long storeId, final ByteString key) throws Exception {
    RunRequest<KeyValueEntry> retry = new RunRequest<KeyValueEntry>(HttpMethod.POST, toUrlPath(storeId, key)) {
      @Override
      protected WebResource addQueryParams(WebResource webResource) {
        return webResource.queryParam("action", "increment");
      }

      protected KeyValueEntry extractResult(ClientResponse response) throws IOException {
        int status = response.getStatus();

        switch (status) {
        case 200:
          InputStream is = response.getEntityInputStream();
          ByteString value = ByteString.readFrom(is);

          return new KeyValueEntry(key, value);

        default:
          throw new IllegalStateException("Unexpected status: " + status);
        }
      }
    };

    return retry.call();
  }

  protected String getErrorInfo(ClientResponse response) {
    try {
      InputStream is = response.getEntityInputStream();
      ByteString value = ByteString.readFrom(is);
      return value.toStringUtf8();
    } catch (Exception e) {
      log.warn("Error while reading error response info", e);
      return null;
    }
  }

  public static class KeyValueEntry {
    private final ByteString key;
    private final ByteString value;

    public KeyValueEntry(ByteString key, ByteString value) {
      this.key = key;
      this.value = value;
    }

    public ByteString getKey() {
      return key;
    }

    public ByteString getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "KeyValueEntry [key=" + Hex.forDebug(key) + ", value=" + Hex.forDebug(value) + "]";
    }

  }

  public KeyValueEntry read(final long storeId, final ByteString key) throws IOException {
    RunRequest<KeyValueEntry> request = new RunRequest<KeyValueEntry>(HttpMethod.GET, toUrlPath(storeId, key)) {

      protected KeyValueEntry extractResult(ClientResponse response) throws IOException {
        int status = response.getStatus();

        switch (status) {
        case 200:
          InputStream is = response.getEntityInputStream();
          ByteString value = ByteString.readFrom(is);
          return new KeyValueEntry(key, value);

        case 404:
          return null;

        default:
          throw new IllegalStateException("Unexpected status: " + status);
        }
      }
    };

    return request.call();
  }

  public KeyValueRecordset query(final long storeId) throws IOException {
    RunRequest<KeyValueRecordset> request = new RunRequest<KeyValueRecordset>(HttpMethod.GET, toUrlPath(storeId)) {

      protected KeyValueRecordset extractResult(final ClientResponse response) throws IOException {
        int status = response.getStatus();

        switch (status) {
        case 200:
          KeyValueRecordset records = new KeyValueRecordset(response);
          this.closeResponse = false;
          return records;

        default:
          throw new IllegalStateException("Unexpected status: " + status);
        }
      }
    };

    return request.call();
  }

  static class KeyValueRecordset extends StreamingRecordsetBase<KeyValueEntry> {
    private final DataInputStream dis;

    public KeyValueRecordset(ClientResponse response) {
      super(new CloseableClientResponse(response));
      InputStream is = response.getEntityInputStream();
      this.dis = new DataInputStream(is);
    }

    @Override
    protected KeyValueEntry read() throws IOException {
      int keyLength = dis.readInt();
      if (keyLength == -1) {
        return null;
      }

      ByteString key = ByteString.readFrom(ByteStreams.limit(dis, keyLength));
      if (key.size() != keyLength) {
        throw new EOFException();
      }

      int valueLength = dis.readInt();
      ByteString value = ByteString.readFrom(ByteStreams.limit(dis, valueLength));
      if (value.size() != valueLength) {
        throw new EOFException();
      }

      return new KeyValueEntry(key, value);
    }
  }

  private String toUrlPath(long storeId, ByteString key) {
    return Long.toString(storeId) + "/" + Hex.toHex(key);
  }

  private String toUrlPath(long storeId) {
    return Long.toString(storeId);
  }

  public void delete(final long storeId, final ByteString key) throws IOException {
    RunRequest<Void> request = new RunRequest<Void>(HttpMethod.DELETE, toUrlPath(storeId, key)) {

      protected Void extractResult(final ClientResponse response) throws IOException {
        int status = response.getStatus();

        switch (status) {
        case 200:
        case 204:
          return null;

        default:
          throw new IllegalStateException("Unexpected status: " + status);
        }
      }
    };

    request.call();
  }

  public String getLeader() {
    return clusterState.getLeader();
  }
}
