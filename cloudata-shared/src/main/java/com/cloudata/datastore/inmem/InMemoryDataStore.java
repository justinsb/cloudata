package com.cloudata.datastore.inmem;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.Modifier;
import com.cloudata.datastore.UniqueIndexViolation;
import com.cloudata.datastore.WhereModifier;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

public class InMemoryDataStore implements DataStore {

  class Space<T extends Message> {
    final T instance;
    final Map<ByteString, T> items = Maps.newHashMap();
    final FieldDescriptor[] primaryKey;

    public Space(T instance, String[] primaryKey) {
      this.instance = instance;
      Descriptor descriptorForType = instance.getDescriptorForType();

      FieldDescriptor[] primaryKeyFields = new FieldDescriptor[primaryKey.length];
      for (int i = 0; i < primaryKey.length; i++) {
        primaryKeyFields[i] = descriptorForType.findFieldByName(primaryKey[i]);
        if (primaryKeyFields[i] == null) {
          throw new IllegalArgumentException("Unknown field: " + primaryKey);
        }
      }
      this.primaryKey = primaryKeyFields;
    }

    public List<T> findAll(T matcher) {
      List<T> matches = Lists.newArrayList();
      Map<FieldDescriptor, Object> matcherFields = matcher.getAllFields();

      for (T item : items.values()) {
        if (matches(matcherFields, item)) {
          matches.add(item);
        }
      }
      return matches;
    }

    public boolean insert(T data, Modifier... modifiers) throws UniqueIndexViolation {
      T key = toKey(data);
      ByteString keyBytes = key.toByteString();

      T existing = items.get(keyBytes);
      if (existing != null) {
        throw new UniqueIndexViolation(null);
      }

      for (Modifier modifier : modifiers) {
        throw new UnsupportedOperationException();
      }

      items.put(keyBytes, data);
      return true;
    }

    public boolean update(T data, Modifier... modifiers) {
      T key = toKey(data);
      ByteString keyBytes = key.toByteString();
      T existing = items.get(keyBytes);
      if (existing == null) {
        return false;
      }
      for (Modifier modifier : modifiers) {
        if (modifier instanceof WhereModifier) {
          WhereModifier where = (WhereModifier) modifier;
          Map<FieldDescriptor, Object> matcherFields = where.getMatcher().getAllFields();
          if (!matches(matcherFields, existing)) {
            return false;
          }
        } else {
          throw new UnsupportedOperationException();
        }
      }
      items.put(keyBytes, data);
      return true;
    }

    public boolean delete(T data, Modifier... modifiers) {
      T key = toKey(data);
      ByteString keyBytes = key.toByteString();
      T existing = items.get(keyBytes);
      if (existing == null) {
        return false;
      }
      for (Modifier modifier : modifiers) {
        if (modifier instanceof WhereModifier) {
          WhereModifier where = (WhereModifier) modifier;
          Map<FieldDescriptor, Object> matcherFields = where.getMatcher().getAllFields();
          if (!matches(matcherFields, existing)) {
            return false;
          }
        } else {
          throw new UnsupportedOperationException();
        }
      }
      items.remove(keyBytes);
      return true;
    }

    private T toKey(T data) {
      Builder b = data.newBuilderForType();
      for (FieldDescriptor field : primaryKey) {
        Object value = data.getField(field);
        b.setField(field, value);
      }
      return (T) b.build();
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

  public <T extends Message> void map(T instance, String... primaryKey) {
    Space<T> space = new Space<T>(instance, primaryKey);
    spaces.put(instance.getClass(), space);
  }

  private <T extends Message> Space<T> getSpace(T instance) {
    synchronized (spaces) {
      Class<T> clazz = (Class<T>) instance.getClass();
      Space<T> space = (Space<T>) spaces.get(clazz);
      if (space == null) {
        throw new IllegalStateException("Unmapped object: " + clazz.getSimpleName());
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
  public <T extends Message> void insert(T data, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(data);
    space.insert(data, modifiers);
  }

  @Override
  public <T extends Message> boolean update(T data, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(data);
    return space.update(data, modifiers);
  }

  @Override
  public <T extends Message> boolean delete(T data, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(data);
    return space.delete(data, modifiers);
  }

}
