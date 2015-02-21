package com.cloudata.datastore;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudata.datastore.DataStore.Mapping;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;

public interface DataStore {

  <T extends Message> Iterable<T> find(T matcher, Modifier... modifiers) throws DataStoreException;

  <T extends Message> Iterable<T> find(T matcher, List<Modifier> modifiers) throws DataStoreException;

  <T extends Message> T findOne(T matcher, Modifier... modifiers) throws DataStoreException;

  <T extends Message> void insert(T data, Modifier... modifiers) throws DataStoreException;

  <T extends Message> void upsert(T data) throws DataStoreException;

  <T extends Message> boolean update(T data, Modifier... modifiers) throws DataStoreException;

  <T extends Message> boolean delete(T data, Modifier... modifiers) throws DataStoreException;

  public static <T extends Message> boolean matches(Map<FieldDescriptor, Object> matcherFields, T item) {
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

  <T extends Message> void addMap(Mapping<T> builder) throws DataStoreException;

  static class Mapping<T extends Message> {
    public final T defaultInstance;
    public List<String> hashKey;
    public List<String> rangeKey;
    public List<String> filterable;

    private Mapping(T defaultInstance) {
      this.defaultInstance = defaultInstance;
      this.hashKey = Lists.newArrayList();
      this.rangeKey = Lists.newArrayList();
      this.filterable = Lists.newArrayList();
    }

    public static <T extends Message> Mapping<T> create(T defaultInstance) {
      return new Mapping<T>(defaultInstance);
    }

    public Mapping<T> rangeKey(String... rangeKey) {
      this.rangeKey = Arrays.asList(rangeKey);
      return this;
    }

    public Mapping<T> filterable(String... filterable) {
      this.filterable = Arrays.asList(filterable);
      return this;
    }

    public Mapping<T> hashKey(String... hashKey) {
      this.hashKey = Arrays.asList(hashKey);
      return this;
    }
  }

}
