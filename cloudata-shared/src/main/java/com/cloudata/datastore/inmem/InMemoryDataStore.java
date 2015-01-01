package com.cloudata.datastore.inmem;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudata.datastore.ComparatorModifier;
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

  public class Space<T extends Message> {
    final T instance;
    final Index primaryKey;
    final List<Index> secondaryIndexes = Lists.newArrayList();
    final Descriptor descriptorForType;

    class Index {
      final FieldDescriptor[] fields;
      final Map<ByteString, T> items = Maps.newHashMap();

      public Index(Descriptor descriptorForType, String[] columns) {
        FieldDescriptor[] fields = new FieldDescriptor[columns.length];
        for (int i = 0; i < columns.length; i++) {
          fields[i] = descriptorForType.findFieldByName(columns[i]);
          if (fields[i] == null) {
            throw new IllegalArgumentException("Unknown field: " + columns[i]);
          }
        }
        this.fields = fields;
      }

      public T buildKey(T data) {
        Builder b = data.newBuilderForType();
        for (FieldDescriptor field : fields) {
          Object value = data.getField(field);
          b.setField(field, value);
        }
        return (T) b.build();
      }
    }

    public Space(T instance, String[] primaryKey) {
      this.instance = instance;
      this.descriptorForType = instance.getDescriptorForType();

      this.primaryKey = new Index(descriptorForType, primaryKey);
    }

    public synchronized List<T> findAll(T matcher, Modifier... modifiers) {
      List<T> matches = Lists.newArrayList();
      Map<FieldDescriptor, Object> matcherFields = matcher.getAllFields();

      for (T item : primaryKey.items.values()) {
        if (matches(matcherFields, item)) {
          boolean isMatch = true;

          for (Modifier modifier : modifiers) {
            if (modifier instanceof ComparatorModifier) {
              ComparatorModifier comparator = (ComparatorModifier) modifier;
              if (!comparator.matches(item)) {
                isMatch = false;
                continue;
              }
              // Map<FieldDescriptor, Object> matcherFields = where.getMatcher().getAllFields();
              // if (!matches(matcherFields, existing)) {
              // return false;
              // }
            } else {
              throw new UnsupportedOperationException();
            }
          }

          if (isMatch) {
            matches.add(item);
          }
        }
      }
      return matches;
    }

    public synchronized boolean insert(T data, Modifier... modifiers) throws UniqueIndexViolation {
      ByteString primaryKeyBytes = primaryKey.buildKey(data).toByteString();

      T existing = primaryKey.items.get(primaryKeyBytes);
      if (existing != null) {
        throw new UniqueIndexViolation(null);
      }

      for (Index index : secondaryIndexes) {
        ByteString indexKey = index.buildKey(data).toByteString();
        existing = index.items.get(indexKey);
        if (existing != null) {
          throw new UniqueIndexViolation(null);
        }
      }

      for (Modifier modifier : modifiers) {
        throw new UnsupportedOperationException();
      }

      primaryKey.items.put(primaryKeyBytes, data);
      for (Index index : secondaryIndexes) {
        ByteString indexKey = index.buildKey(data).toByteString();
        index.items.put(indexKey, data);
      }

      return true;
    }

    public synchronized boolean update(T data, Modifier... modifiers) throws UniqueIndexViolation {
      ByteString primaryKeyBytes = primaryKey.buildKey(data).toByteString();

      T existing = primaryKey.items.get(primaryKeyBytes);
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

      for (Index secondaryIndex : secondaryIndexes) {
        ByteString indexKey = secondaryIndex.buildKey(data).toByteString();
        existing = secondaryIndex.items.get(indexKey);
        if (existing != null) {
          throw new UniqueIndexViolation(null);
        }
      }

      primaryKey.items.put(primaryKeyBytes, data);
      for (Index index : secondaryIndexes) {
        ByteString oldKey = index.buildKey(existing).toByteString();
        index.items.remove(oldKey);
        ByteString newKey = index.buildKey(existing).toByteString();
        index.items.put(newKey, data);
      }

      return true;
    }

    public synchronized boolean delete(T data, Modifier... modifiers) {
      ByteString primaryKeyBytes = primaryKey.buildKey(data).toByteString();

      T existing = primaryKey.items.get(primaryKeyBytes);
      if (existing == null) {
        return false;
      }

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
      primaryKey.items.remove(primaryKeyBytes);
      for (Index secondaryIndex : secondaryIndexes) {
        ByteString oldKey = secondaryIndex.buildKey(existing).toByteString();
        secondaryIndex.items.remove(oldKey);
      }
      return true;
    }

    public void withIndex(String... columns) {
      secondaryIndexes.add(new Index(descriptorForType, columns));
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

  public <T extends Message> Space<T> map(T instance, String... primaryKey) {
    Space<T> space = new Space<T>(instance, primaryKey);
    spaces.put(instance.getClass(), space);
    return space;
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
  public <T extends Message> Iterable<T> find(T matcher, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(matcher);
    return space.findAll(matcher, modifiers);
  }

  @Override
  public <T extends Message> T findOne(T matcher, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(matcher);
    List<T> matches = space.findAll(matcher, modifiers);
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
