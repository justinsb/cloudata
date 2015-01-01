package com.cloudata.datastore.inmem;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.Modifier;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

public class InMemoryDataStore implements DataStore {

  class Space<T extends Message> {
    final Class<T> clazz;
    final List<T> items = Lists.newArrayList();

    public Space(Class<T> clazz) {
      super();
      this.clazz = clazz;
    }

    public List<T> findAll(T matcher) {
      List<T> matches = Lists.newArrayList();
      Map<FieldDescriptor, Object> matcherFields = matcher.getAllFields();

      for (T item : items) {
        if (matches(matcherFields, item)) {
          matches.add(item);
        }
      }
      return matches;
    }

    public T insert(T data) {
      items.add(data);
      return data;
    }
  }

  private static <T extends Message> boolean matches(Map<FieldDescriptor, Object> matcherFields, T item) {
    for (Entry<FieldDescriptor, Object> entry : matcherFields.entrySet()) {
      FieldDescriptor field = entry.getKey();
      Object matcherValue = entry.getValue();

      if (!item.hasField(field)) {
        return false;
      }

      Object itemValue = item.getField(field);
      if (!Objects.equal(itemValue, matcherValue)) {
        return false;
      }
    }
    return true;
  }

  final Map<Class<?>, Space<?>> spaces = Maps.newHashMap();

  private <T extends Message> Space<T> getSpace(T instance) {
    synchronized (spaces) {
      Class<T> clazz = (Class<T>) instance.getClass();
      Space<T> space = (Space<T>) spaces.get(clazz);
      if (space == null) {
        space = new Space<T>(clazz);
        spaces.put(clazz, space);
      }
      return space;
    }
  }

  @Override
  public <T extends Message> Iterable<T> find(T matcher) throws DataStoreException {
    Space<T> space = getSpace(matcher);
    return space.findAll(matcher);
  }

  @Override
  public <T extends Message> T findOne(T matcher) throws DataStoreException {
    Space<T> space = getSpace(matcher);
    List<T> matches = space.findAll(matcher);
    if (matches.size() == 1) {
      return matches.get(0);
    }
    if (matches.size() == 0) {
      return null;
    }
    throw new IllegalStateException("Found multiple matches");
  }

  @Override
  public <T extends Message> T insert(T data, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(data);
    return space.insert(data);
  }

  @Override
  public <T extends Message> boolean update(T data, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(data);
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Message> boolean delete(T data, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(data);
    throw new UnsupportedOperationException();
  }

}
