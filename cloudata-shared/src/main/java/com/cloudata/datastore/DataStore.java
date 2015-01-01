package com.cloudata.datastore;

import java.util.Queue;

import com.google.protobuf.Message;

public interface DataStore {

  <T extends Message> Iterable<T> find(T matcher) throws DataStoreException;

  <T extends Message> T findOne(T matcher) throws DataStoreException;

  <T extends Message> T insert(T data, Modifier... modifiers) throws DataStoreException;

  <T extends Message> boolean update(T data, Modifier... modifiers) throws DataStoreException;

  <T extends Message> boolean delete(T data, Modifier... modifiers) throws DataStoreException;

}
