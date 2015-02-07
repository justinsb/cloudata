package com.cloudata.datastore.inmem;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudata.datastore.ComparatorModifier;
import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.LimitModifier;
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
      final boolean requireFields;

      public Index(Descriptor descriptorForType, String[] columns, boolean requireFields) {
        this.requireFields = requireFields;
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
          if (!data.hasField(field)) {
            if (requireFields) {
              throw new IllegalStateException("Field not set: " + field.getFullName());
            } else {
              continue;
            }
          }
          Object value = data.getField(field);
          b.setField(field, value);
        }
        return (T) b.build();
      }
    }

    public Space(DataStore.Mapping<T> builder) {
      throw new UnsupportedOperationException();
      // this.instance = instance;
      // this.descriptorForType = instance.getDescriptorForType();
      //
      // this.primaryKey = new Index(descriptorForType, primaryKey, true);
    }

    public synchronized List<T> findAll(T matcher, List<Modifier> modifiers) {
      List<T> matches = Lists.newArrayList();
      Map<FieldDescriptor, Object> matcherFields = matcher.getAllFields();

      int limit = Integer.MAX_VALUE;

      // We could optimize this further, but this is not really production code!
      for (Modifier modifier : modifiers) {
        if (modifier instanceof ComparatorModifier) {
          continue;
        } else if (modifier instanceof LimitModifier) {
          LimitModifier limitModifier = (LimitModifier) modifier;
          limit = limitModifier.getLimit();
        } else {
          throw new UnsupportedOperationException();
        }
      }

      for (T item : primaryKey.items.values()) {
        if (DataStore.matches(matcherFields, item)) {
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
            }
          }

          if (isMatch) {
            matches.add(item);
            if (matches.size() >= limit) {
              break;
            }
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

    public synchronized void upsert(T data) throws UniqueIndexViolation {
      if (!update(data)) {
        insert(data);
      }
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
          if (!DataStore.matches(matcherFields, existing)) {
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
          if (!DataStore.matches(matcherFields, existing)) {
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
      secondaryIndexes.add(new Index(descriptorForType, columns, false));
    }

  }

  final Map<Class<?>, Space<?>> spaces = Maps.newHashMap();

  public <T extends Message> void addMapping(DataStore.Mapping<T> builder) {
    Space<T> space = new Space<T>(builder);
    spaces.put(space.instance.getClass(), space);
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
    return find(matcher, Arrays.asList(modifiers));
  }

  @Override
  public <T extends Message> Iterable<T> find(T matcher, List<Modifier> modifiers) throws DataStoreException {
    Space<T> space = getSpace(matcher);
    return space.findAll(matcher, modifiers);
  }

  @Override
  public <T extends Message> T findOne(T matcher, Modifier... modifiers) throws DataStoreException {
    Space<T> space = getSpace(matcher);

    List<Modifier> modifiersWithLimit = Lists.newArrayList(modifiers);
    modifiersWithLimit.add(new LimitModifier(2));
    List<T> matches = space.findAll(matcher, modifiersWithLimit);

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

  @Override
  public <T extends Message> void upsert(T data) throws DataStoreException {
    Space<T> space = getSpace(data);
    space.upsert(data);
  }

  @Override
  public <T extends Message> void addMap(DataStore.Mapping<T> builder) {
    throw new UnsupportedOperationException();

  }

}
