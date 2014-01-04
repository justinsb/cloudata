//package com.cloudata.blockstore.web;
//
//import javax.inject.Inject;
//import javax.ws.rs.Path;
//import javax.ws.rs.PathParam;
//
//import com.cloudata.blockstore.KeyValueStateMachine;
//
//@Path("/{storeId}/")
//public class KeyValueEndpoint {
//
//    @Inject
//    KeyValueStateMachine stateMachine;
//
//    @PathParam("storeId")
//    long storeId;
//
//    // @GET
//    // @Path("{key}")
//    // // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
//    // public Response get(@PathParam("key") String key) throws IOException {
//    // byte[] k = BaseEncoding.base16().decode(key);
//    //
//    // Value v = stateMachine.get(storeId, getKeyspace(), ByteString.copyFrom(k));
//    //
//    // if (v == null) {
//    // return Response.status(Status.NOT_FOUND).build();
//    // }
//    //
//    // ByteBuffer data = v.asBytes();
//    // return Response.ok(data).build();
//    // }
//    //
//    // @GET
//    // @Produces(MediaType.APPLICATION_OCTET_STREAM)
//    // public Response query() throws IOException {
//    // BtreeQuery query = stateMachine.scan(storeId, getKeyspace());
//    //
//    // query.setFormat(MediaType.APPLICATION_OCTET_STREAM_TYPE);
//    // return Response.ok(query).build();
//    // }
//    //
//    // enum PostAction {
//    // SET, INCREMENT
//    // }
//    //
//    // @POST
//    // @Path("{key}")
//    // // @Consumes(MediaType.APPLICATION_OCTET_STREAM)
//    // public Response post(@PathParam("key") String key, @QueryParam("action") String actionString,
//    // InputStream valueStream) throws IOException {
//    // try {
//    // ByteString k = ByteString.copyFrom(BaseEncoding.base16().decode(key));
//    // byte[] v = ByteStreams.toByteArray(valueStream);
//    //
//    // Keyspace keyspace = getKeyspace();
//    // ByteString qualifiedKey = keyspace.mapToKey(k);
//    // KvAction action = KvAction.SET;
//    //
//    // if (actionString != null) {
//    // actionString = actionString.toUpperCase();
//    // action = KvAction.valueOf(actionString);
//    // }
//    //
//    // Object ret;
//    //
//    // switch (action) {
//    // case SET: {
//    // Value value = Value.fromRawBytes(v);
//    // SetOperation operation = new SetOperation(qualifiedKey, value);
//    // stateMachine.doAction(storeId, operation);
//    // ret = null;
//    // break;
//    // }
//    //
//    // case INCREMENT: {
//    // IncrementOperation operation = new IncrementOperation(qualifiedKey, 1);
//    // Long newValue = stateMachine.doAction(storeId, operation);
//    // ret = Value.fromLong(newValue).asBytes();
//    // break;
//    // }
//    //
//    // default:
//    // throw new IllegalArgumentException();
//    // }
//    //
//    // return Response.ok(ret).build();
//    // } catch (InterruptedException e) {
//    // return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
//    // } catch (NoLeaderException e) {
//    // return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
//    // } catch (NotLeaderException e) {
//    // Replica leader = e.getLeader();
//    // InetSocketAddress address = (InetSocketAddress) leader.address();
//    // URI uri = URI.create("http://" + address.getHostName() + ":" + address.getPort() /* + "/" + key */);
//    // System.out.println(uri);
//    // return Response.seeOther(uri).build();
//    // } catch (RaftException e) {
//    // return Response.serverError().build();
//    // }
//    // }
//    //
//    // @DELETE
//    // @Path("{key}")
//    // public Response delete(@PathParam("key") String key) throws IOException {
//    // try {
//    // ByteString k = ByteString.copyFrom(BaseEncoding.base16().decode(key));
//    //
//    // Keyspace keyspace = getKeyspace();
//    // ByteString qualifiedKey = keyspace.mapToKey(k);
//    //
//    // stateMachine.doAction(storeId, new DeleteOperation(qualifiedKey));
//    //
//    // return Response.noContent().build();
//    // } catch (InterruptedException e) {
//    // return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
//    // } catch (NoLeaderException e) {
//    // return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
//    // } catch (NotLeaderException e) {
//    // Replica leader = e.getLeader();
//    // InetSocketAddress address = (InetSocketAddress) leader.address();
//    // URI uri = URI.create("http://" + address.getHostName() + ":" + address.getPort() /* + "/" + key */);
//    // System.out.println(uri);
//    // return Response.seeOther(uri).build();
//    // } catch (RaftException e) {
//    // return Response.serverError().build();
//    // }
//    // }
//    //
//    // private Keyspace getKeyspace() {
//    // return Keyspace.ZERO;
//    // }
//
// }
