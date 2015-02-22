package com.cloudata.datastore.dynamodb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Attribute;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ConditionalOperator;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.cloudata.MetadataModel.TypeMetadata;
import com.cloudata.datastore.ComparatorModifier;
import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.LimitModifier;
import com.cloudata.datastore.Modifier;
import com.cloudata.datastore.UniqueIndexViolation;
import com.cloudata.datastore.WhereModifier;
import com.cloudata.datastore.sql.SqlDataStore;
import com.cloudata.util.ByteBufferInputStream;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;

public class DynamodbDataStore implements DataStore {

  private static final String DEFAULT_DYNAMO_TABLENAME = "misc_store";
  private static final String FIELD_HASH_KEY = "hash_key";
  private static final String FIELD_RANGE_KEY = "range_key";
  private static final String FIELD_BLOB = "blob";

  private static final int SYSTEM_ID_TYPE_METADATA = 1;
  private static final int SYSTEM_ID_MAX = 32;

  private static final ByteString EMPTY_RANGE_KEY = ByteString.copyFrom(new byte[] { 0 });

  private static final Logger log = LoggerFactory.getLogger(SqlDataStore.class);

  final AmazonDynamoDBClient dynamoDB;

  final Map<Class<?>, DynamoClassMapping<?>> mappings = Maps.newHashMap();

  final DynamoClassMapping<TypeMetadata> METADATA_MAPPING = buildMetadataMapping();

  public DynamodbDataStore(AWSCredentialsProvider awsCredentialsProvider) {
    // this.dynamoDB = new DynamoDB(new AmazonDynamoDBClient(awsCredentialsProvider));
    this.dynamoDB = new AmazonDynamoDBClient(awsCredentialsProvider);

    this.mappings.put(TypeMetadata.class, METADATA_MAPPING);
  }

  private DynamoClassMapping<TypeMetadata> buildMetadataMapping() {
    Mapping<TypeMetadata> builder = DataStore.Mapping.create(TypeMetadata.getDefaultInstance());
    builder.hashKey = Arrays.asList("id");
    TypeMetadata typeMetadata = TypeMetadata.newBuilder().setId(SYSTEM_ID_TYPE_METADATA)
        .setTypeDescriptor(TypeMetadata.getDescriptor().toProto()).build();
    return new DynamoClassMapping<TypeMetadata>(DEFAULT_DYNAMO_TABLENAME, builder, typeMetadata);
  }

  private TypeMetadata findTypeMetadata(Descriptor descriptor) throws DataStoreException {
    // TODO: Allow override?
    String name = descriptor.getFullName();

    log.debug("Looking up class id for {}", name);

    byte[] bytes = name.getBytes(Charsets.UTF_8);

    int modulo = 0xffff;

    int n = Math.abs(Hashing.murmur3_32().hashBytes(bytes).asInt()) % modulo;
    int seed = 0xda4aba5e;
    int skip = Math.abs(Hashing.murmur3_32(seed).hashBytes(bytes).asInt());
    // Avoid problematic skips; 0 would be bad obviously
    // We probably don't need to skip small values, but it can't really hurt
    if (skip < 8) {
      skip = 8;
    }

    while (true) {
      if (n > SYSTEM_ID_MAX) {
        TypeMetadata matcher = TypeMetadata.newBuilder().setId(n).build();
        TypeMetadata typeMetadata = findOne(matcher);
        if (typeMetadata == null) {
          // Need to insert
          TypeMetadata.Builder b = TypeMetadata.newBuilder();
          b.setId(n);
          b.setName(name);
          b.setTypeDescriptor(descriptor.toProto());
          typeMetadata = b.build();
          // TODO: Retry on concurrent create?
          insert(typeMetadata);
          return typeMetadata;
        }

        if (typeMetadata.getName().equals(name)) {
          return typeMetadata;
        }
      }

      n = (n + skip) % modulo;
    }
  }

  <T extends Message> DynamoClassMapping<T> getClassMapping(T instance) throws DataStoreException {
    Class<T> clazz = (Class<T>) instance.getClass();
    DynamoClassMapping<T> mapping = (DynamoClassMapping<T>) mappings.get(clazz);
    if (mapping == null) {
      throw new IllegalStateException("Unmapped object: " + clazz.getSimpleName());
    }
    return mapping;
  }

  @Override
  public <T extends Message> Iterable<T> find(T matcher, Modifier... modifiers) throws DataStoreException {
    List<T> results = findMatching(matcher, Arrays.asList(modifiers));
    return results;
  }

  @Override
  public <T extends Message> Iterable<T> find(T matcher, List<Modifier> modifiers) throws DataStoreException {
    List<T> results = findMatching(matcher, modifiers);
    return results;
  }

  @Override
  public <T extends Message> T findOne(T matcher, Modifier... modifiers) throws DataStoreException {
    DynamoClassMapping<T> tableInfo = getClassMapping(matcher);

    log.debug("findOne {} {}", matcher.getClass().getSimpleName(), matcher);

    Map<FieldDescriptor, Object> matcherFields = matcher.getAllFields();
    // TODO: Remove PK fields

    GetItemRequest request = new GetItemRequest();
    // TODO: Modifier for eventually consistent read?
    request.setConsistentRead(true);
    request.setTableName(tableInfo.getDynamoTableName());
    request.setKey(tableInfo.buildCompleteKey(matcher));

    for (Modifier modifier : modifiers) {
      throw new UnsupportedOperationException();
    }

    GetItemResult result = dynamoDB.getItem(request);
    Map<String, AttributeValue> itemData = result.getItem();
    if (itemData == null) {
      return null;
    }

    T item = tableInfo.mapFromDb(itemData);
    if (DataStore.matches(matcherFields, item)) {
      log.debug("found item: {}", item);
      return item;
    }

    return null;
  }

  private <T extends Message> List<T> scan(T matcher, List<Modifier> modifiers) throws DataStoreException {
    DynamoClassMapping<T> tableInfo = getClassMapping(matcher);

    log.debug("scan {} {}", matcher.getClass().getSimpleName(), matcher);

    AttributeValue hashKey = tableInfo.buildHashKey(matcher);
    if (hashKey != null) {
      // We don't need to do a scan here!
      throw new IllegalStateException();
    }

    Map<FieldDescriptor, Object> matcherFields = matcher.getAllFields();
    // TODO: Remove PK fields

    ScanRequest request = new ScanRequest();
    request.setTableName(tableInfo.getDynamoTableName());

    // TODO: Filter expressions!

    // int limit = Integer.MAX_VALUE;

    for (Modifier modifier : modifiers) {
      if (modifier instanceof ComparatorModifier) {
        throw new UnsupportedOperationException();
      } else if (modifier instanceof LimitModifier) {
        throw new UnsupportedOperationException();
        // limitModifier = (LimitModifier) modifier;
        // limit = limitModifier.getLimit();
      } else {
        throw new UnsupportedOperationException();
      }
    }

    ScanResult response = dynamoDB.scan(request);

    Map<String, AttributeValue> lastEvaluatedKey = response.getLastEvaluatedKey();
    if (lastEvaluatedKey != null) {
      throw new UnsupportedOperationException("Multiple page results not implemented");
    }

    List<T> items = Lists.newArrayList();
    List<Map<String, AttributeValue>> responseItems = response.getItems();
    for (Map<String, AttributeValue> itemData : responseItems) {
      if (!tableInfo.matchesType(itemData)) {
        continue;
      }

      T item = tableInfo.mapFromDb(itemData);
      if (!DataStore.matches(matcherFields, matcher)) {
        continue;
      }
      items.add(item);
    }

    return items;
  }

  public <T extends Message> List<T> reindex(T instance) throws DataStoreException {
    DynamoClassMapping<T> tableInfo = getClassMapping(instance);

    log.debug("reindex {}", instance.getClass().getSimpleName());

    ScanRequest scanRequest = new ScanRequest();
    scanRequest.setTableName(tableInfo.getDynamoTableName());

    // TODO: Filter expressions on prefix?

    ScanResult scanResponse = dynamoDB.scan(scanRequest);

    Map<String, AttributeValue> lastEvaluatedKey = scanResponse.getLastEvaluatedKey();
    if (lastEvaluatedKey != null) {
      throw new UnsupportedOperationException("Multiple page results not implemented");
    }

    List<T> items = Lists.newArrayList();
    List<Map<String, AttributeValue>> responseItems = scanResponse.getItems();
    for (Map<String, AttributeValue> itemData : responseItems) {
      if (!tableInfo.matchesType(itemData)) {
        continue;
      }

      T item = tableInfo.mapFromDb(itemData);

      Map<String, AttributeValue> newItemData = tableInfo.mapToDb(item);

      if (DynamoDbHelpers.areEqual(itemData, newItemData)) {
        log.debug("No change for item: {}", itemData);
        continue;
      }

      PutItemRequest putRequest = new PutItemRequest();
      putRequest.setTableName(tableInfo.getDynamoTableName());
      putRequest.setItem(itemData);
      dynamoDB.putItem(putRequest);

      Map<String, AttributeValue> oldKey = extractKey(itemData);
      Map<String, AttributeValue> newKey = extractKey(newItemData);

      if (!DynamoDbHelpers.areEqual(oldKey, newKey)) {
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest();
        deleteItemRequest.setTableName(tableInfo.getDynamoTableName());
        deleteItemRequest.setKey(oldKey);
        dynamoDB.deleteItem(deleteItemRequest);
      }

    }

    return items;
  }

  private static Map<String, AttributeValue> extractKey(Map<String, AttributeValue> itemData) {
    Map<String, AttributeValue> key = Maps.newHashMap();
    if (itemData.containsKey(FIELD_HASH_KEY)) {
      key.put(FIELD_HASH_KEY, itemData.get(FIELD_HASH_KEY));
    }
    if (itemData.containsKey(FIELD_RANGE_KEY)) {
      key.put(FIELD_RANGE_KEY, itemData.get(FIELD_RANGE_KEY));
    }
    return key;
  }

  private <T extends Message> List<T> findMatching(T matcher, List<Modifier> modifiers) throws DataStoreException {
    DynamoClassMapping<T> tableInfo = getClassMapping(matcher);

    log.debug("findAll {} {}", matcher.getClass().getSimpleName(), matcher);

    AttributeValue hashKey = tableInfo.buildHashKey(matcher);
    if (hashKey == null) {
      log.warn("No hash-query provided for query, full table scan required: {}", matcher);
      // XXX: throw?
      return scan(matcher, modifiers);
    }

    Map<FieldDescriptor, Object> matcherFields = matcher.getAllFields();
    // TODO: Remove PK fields

    QueryRequest request = new QueryRequest();
    // TODO: Modifier for eventually consistent read?
    request.setConsistentRead(true);
    request.setTableName(tableInfo.getDynamoTableName());

    Map<String, Condition> keyConditions = Maps.newHashMap();
    keyConditions.put(FIELD_HASH_KEY, new Condition().withComparisonOperator(ComparisonOperator.EQ)
        .withAttributeValueList(hashKey));

    tableInfo.addRangeKeyCondition(matcher, keyConditions);

    request.setKeyConditions(keyConditions);

    // int limit = Integer.MAX_VALUE;

    for (Modifier modifier : modifiers) {
      if (modifier instanceof ComparatorModifier) {
        throw new UnsupportedOperationException();
      } else if (modifier instanceof LimitModifier) {
        throw new UnsupportedOperationException();
        // limitModifier = (LimitModifier) modifier;
        // limit = limitModifier.getLimit();
      } else {
        throw new UnsupportedOperationException();
      }
    }

    QueryResult response = dynamoDB.query(request);

    Map<String, AttributeValue> lastEvaluatedKey = response.getLastEvaluatedKey();
    if (lastEvaluatedKey != null) {
      throw new UnsupportedOperationException("Multiple page results not implemented");
    }

    List<T> items = Lists.newArrayList();
    List<Map<String, AttributeValue>> responseItems = response.getItems();
    for (Map<String, AttributeValue> itemData : responseItems) {
      T item = tableInfo.mapFromDb(itemData);
      if (!DataStore.matches(matcherFields, matcher)) {
        continue;
      }
      items.add(item);
    }

    return items;
  }

  @Override
  public <T extends Message> void insert(T item, Modifier... modifiers) throws DataStoreException {
    DynamoClassMapping<T> tableInfo = getClassMapping(item);

    log.debug("Insert {} {}", item.getClass().getSimpleName(), item);
    for (Modifier modifier : modifiers) {
      throw new UnsupportedOperationException();
    }

    PutItemRequest request = new PutItemRequest();
    request.setTableName(tableInfo.getDynamoTableName());

    Map<String, AttributeValue> itemData = tableInfo.mapToDb(item);
    request.setItem(itemData);

    Map<String, ExpectedAttributeValue> expected = Maps.newHashMap();
    expected.put(FIELD_HASH_KEY, new ExpectedAttributeValue().withComparisonOperator(ComparisonOperator.NULL));
    request.setExpected(expected);
    if (expected.size() > 1) {
      request.setConditionalOperator(ConditionalOperator.AND);
    }

    try {
      dynamoDB.putItem(request);
    } catch (ConditionalCheckFailedException e) {
      log.debug("Insert failed {}", item, e);

      throw new UniqueIndexViolation(null);
    }
  }

  @Override
  public <T extends Message> boolean update(T item, Modifier... modifiers) throws DataStoreException {
    DynamoClassMapping<T> tableInfo = getClassMapping(item);

    log.debug("Update {} {} [{}]", item.getClass().getSimpleName(), item, modifiers);

    UpdateItemRequest request = new UpdateItemRequest();
    request.setTableName(tableInfo.getDynamoTableName());
    request.setKey(tableInfo.buildCompleteKey(item));

    Map<String, ExpectedAttributeValue> expected = Maps.newHashMap();
    expected.put(FIELD_HASH_KEY, new ExpectedAttributeValue().withComparisonOperator(ComparisonOperator.NOT_NULL));

    for (Modifier modifier : modifiers) {
      if (modifier instanceof WhereModifier) {
        WhereModifier where = (WhereModifier) modifier;
        Map<FieldDescriptor, Object> matcherFields = where.getMatcher().getAllFields();
        for (Map.Entry<FieldDescriptor, Object> matcherField : matcherFields.entrySet()) {
          FieldDescriptor fieldDescriptor = matcherField.getKey();
          Object fieldValue = matcherField.getValue();
          tableInfo.addFilter(expected, fieldDescriptor, fieldValue);
        }
      } else {
        throw new UnsupportedOperationException();
      }
    }

    Map<String, AttributeValueUpdate> attributeUpdates = tableInfo.mapToUpdate(item);

    request.setAttributeUpdates(attributeUpdates);
    request.setExpected(expected);
    if (expected.size() > 1) {
      request.setConditionalOperator(ConditionalOperator.AND);
    }

    try {
      UpdateItemResult response = dynamoDB.updateItem(request);
      return true;
    } catch (ConditionalCheckFailedException e) {
      log.debug("Update failed (conditional check failed)");
      return false;
    }
  }

  @Override
  public <T extends Message> boolean delete(T item, Modifier... modifiers) throws DataStoreException {
    DynamoClassMapping<T> tableInfo = getClassMapping(item);

    log.debug("Delete {}", item);

    for (Modifier modifier : modifiers) {
      throw new UnsupportedOperationException();
    }

    DeleteItemRequest request = new DeleteItemRequest();
    request.setTableName(tableInfo.getDynamoTableName());
    request.setKey(tableInfo.buildCompleteKey(item));
    request.setConditionExpression("attribute_exists(hash_key)");
    try {
      DeleteItemResult response = dynamoDB.deleteItem(request);
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  @Override
  public <T extends Message> void upsert(T data) throws DataStoreException {
    throw new UnsupportedOperationException();
  }

  private static AttributeValue toAttributeValue(ByteString bb) {
    return new AttributeValue().withB(bb.asReadOnlyByteBuffer());
  }

  private static AttributeValue toAttributeValue(long v) {
    return new AttributeValue().withN(Long.toString(v));
  }

  public static class DynamoClassMapping<T extends Message> {

    final String dynamoTableName;

    final T defaultInstance;
    final List<AttributeMapping> hashKeyFields;
    final List<AttributeMapping> rangeKeyFields;
    final List<AttributeMapping> filterable;
    final Message.Builder builderPrototype;

    final TypeMetadata typeMetadata;

    final Descriptor descriptor;

    final Map<Integer, AttributeMapping> attributeMappings;

    static class AttributeMapping {
      String attributeName;
      boolean isHashKey;
      boolean isRangeKey;
      boolean isFilterable;
      FieldDescriptor field;

      public AttributeValue buildAttributeValue(Object value) {
        Type type = field.getType();
        if (type == Type.BYTES) {
          ByteString b = (ByteString) value;
          AttributeValue attributeValue = new AttributeValue().withB(b.asReadOnlyByteBuffer());
          return attributeValue;
          // AttributeValue attributeValue = tableInfo.mapToAttributeValue(fieldDescriptor, fieldValue);
          // expected.put(attributeName, new ExpectedAttributeValue(toAttributeValue(where.getVersion())));
        } else {
          throw new UnsupportedOperationException("Unsupport field type: " + type);
        }
      }
    }

    private DynamoClassMapping(String dynamoTableName, Mapping<T> builder, TypeMetadata typeMetadata) {
      this.typeMetadata = typeMetadata;
      this.descriptor = builder.defaultInstance.getDescriptorForType();
      this.dynamoTableName = dynamoTableName;
      this.defaultInstance = builder.defaultInstance;
      this.builderPrototype = builder.defaultInstance.newBuilderForType();
      List<AttributeMapping> hashKeyFields = Lists.newArrayList();
      List<AttributeMapping> rangeKeyFields = Lists.newArrayList();
      List<AttributeMapping> filterable = Lists.newArrayList();

      Map<Integer, AttributeMapping> attributeMappings = Maps.newHashMap();
      for (FieldDescriptor field : this.descriptor.getFields()) {
        AttributeMapping attributeMapping = new AttributeMapping();
        attributeMapping.field = field;
        attributeMapping.attributeName = field.getName();
        attributeMappings.put(field.getNumber(), attributeMapping);
      }

      for (String fieldName : builder.hashKey) {
        FieldDescriptor field = this.descriptor.findFieldByName(fieldName);
        if (field == null) {
          throw new IllegalStateException("Field not found: " + fieldName);
        }

        AttributeMapping attributeMapping = attributeMappings.get(field.getNumber());
        attributeMapping.isHashKey = true;

        hashKeyFields.add(attributeMapping);
      }

      for (String fieldName : builder.rangeKey) {
        FieldDescriptor field = this.descriptor.findFieldByName(fieldName);
        if (field == null) {
          throw new IllegalStateException("Field not found: " + fieldName);
        }

        AttributeMapping attributeMapping = attributeMappings.get(field.getNumber());
        attributeMapping.isRangeKey = true;

        rangeKeyFields.add(attributeMapping);
      }

      for (int i = 1; i < rangeKeyFields.size(); i++) {
        AttributeMapping previous = rangeKeyFields.get(i - 1);
        AttributeMapping current = rangeKeyFields.get(i);
        if (previous.field.getIndex() >= current.field.getIndex()) {
          // Currently we serialize in structure order (default protobuf), not field order
          throw new UnsupportedOperationException();
        }
      }
      for (String fieldName : builder.filterable) {
        FieldDescriptor field = this.descriptor.findFieldByName(fieldName);
        if (field == null) {
          throw new IllegalStateException("Field not found: " + fieldName);
        }

        AttributeMapping attributeMapping = attributeMappings.get(field.getNumber());
        attributeMapping.isFilterable = true;

        filterable.add(attributeMapping);
      }

      this.attributeMappings = attributeMappings;
      this.hashKeyFields = ImmutableList.copyOf(hashKeyFields);
      this.rangeKeyFields = ImmutableList.copyOf(rangeKeyFields);
      this.filterable = ImmutableList.copyOf(filterable);
    }

    public boolean matchesType(Map<String, AttributeValue> fields) {
      AttributeValue hashKey = fields.get(FIELD_HASH_KEY);
      if (hashKey == null) {
        throw new IllegalArgumentException();
      }
      ByteBuffer b = hashKey.getB();
      if (b == null) {
        throw new IllegalArgumentException();
      }

      ByteString bytes = ByteString.copyFrom(b.asReadOnlyBuffer());
      ByteString prefix = ByteString.copyFrom(Ints.toByteArray(typeMetadata.getId()));

      return bytes.startsWith(prefix);
    }

    public void addFilter(Map<String, ExpectedAttributeValue> expected, FieldDescriptor fieldDescriptor,
        Object fieldValue) throws DataStoreException {

      AttributeMapping attributeMapping = getAttributeMapping(fieldDescriptor);
      if (attributeMapping.isHashKey || attributeMapping.isRangeKey) {
        // Skip; we assume that the caller will use the filter
        return;
      }

      if (!attributeMapping.isFilterable) {
        throw new DataStoreException("Field not extracted: " + fieldDescriptor.getName());
      }

      AttributeValue attributeValue = attributeMapping.buildAttributeValue(fieldValue);
      expected.put(attributeMapping.attributeName,
          new ExpectedAttributeValue(attributeValue).withComparisonOperator(ComparisonOperator.EQ));
    }

    private AttributeMapping getAttributeMapping(FieldDescriptor fieldDescriptor) {
      if (fieldDescriptor.getContainingType() != this.descriptor) {
        throw new IllegalStateException();
      }

      AttributeMapping attributeMapping = attributeMappings.get(fieldDescriptor.getNumber());
      if (attributeMapping == null) {
        throw new IllegalStateException();
      }
      return attributeMapping;
    }

    public T mapFromDb(Map<String, AttributeValue> data) {
      Message.Builder dest = newBuilder();
      AttributeValue blob = data.get(FIELD_BLOB);
      if (blob == null) {
        throw new IllegalArgumentException("Expected blob column");
      }
      try {
        dest.mergeFrom(new ByteBufferInputStream(blob.getB().asReadOnlyBuffer()));
      } catch (IOException e) {
        throw new IllegalArgumentException("Error reading blob", e);
      }
      return (T) dest.build();
    }

    public Map<String, AttributeValue> mapToDb(T item) {
      Map<String, AttributeValue> data = Maps.newHashMap();
      AttributeValue hashKey = buildHashKey(item);
      data.put(FIELD_HASH_KEY, hashKey);

      if (!hasRangeKey()) {
        // Range key is required by dynamodb; add dummy value
        data.put(FIELD_RANGE_KEY, toAttributeValue(EMPTY_RANGE_KEY));
      } else {
        RangeSpecifier rangeKey = buildRangeKey(item);
        if (rangeKey == null || rangeKey.exact == null) {
          throw new IllegalArgumentException();
        }
        data.put(FIELD_RANGE_KEY, rangeKey.exact);
      }

      data.put(FIELD_BLOB, buildBlob(item));

      for (AttributeMapping filterableField : this.filterable) {
        if (!item.hasField(filterableField.field)) {
          continue;
        }
        Object value = item.getField(filterableField.field);
        data.put(filterableField.attributeName, filterableField.buildAttributeValue(value));
      }
      return data;
    }

    public Map<String, AttributeValueUpdate> mapToUpdate(T item) {
      Map<String, AttributeValueUpdate> attributeUpdates = Maps.newHashMap();
      attributeUpdates.put(FIELD_BLOB, new AttributeValueUpdate(buildBlob(item), AttributeAction.PUT));
      for (AttributeMapping filterableField : this.filterable) {
        if (item.hasField(filterableField.field)) {
          Object value = item.getField(filterableField.field);
          AttributeValueUpdate update = new AttributeValueUpdate(filterableField.buildAttributeValue(value),
              AttributeAction.PUT);
          attributeUpdates.put(filterableField.attributeName, update);
        } else {
          AttributeValueUpdate update = new AttributeValueUpdate().withAction(AttributeAction.DELETE);
          attributeUpdates.put(filterableField.attributeName, update);
        }
      }
      return attributeUpdates;

    }

    public AttributeValue buildBlob(T item) {
      return new AttributeValue().withB(item.toByteString().asReadOnlyByteBuffer());
    }

    public AttributeValue buildHashKey(T item) {
      boolean empty = true;

      Message.Builder key = newBuilder();
      for (AttributeMapping hashKeyField : hashKeyFields) {
        if (item.hasField(hashKeyField.field)) {
          Object value = item.getField(hashKeyField.field);
          key.setField(hashKeyField.field, value);
          empty = false;
        }
      }

      if (empty) {
        return null;
      }

      try {
        Output output = ByteString.newOutput();

        output.write(Ints.toByteArray(typeMetadata.getId()));
        key.buildPartial().writeTo(output);

        return toAttributeValue(output.toByteString());
      } catch (IOException e) {
        throw new IllegalArgumentException("Error building hash key", e);
      }
    }

    public static class RangeSpecifier {
      AttributeValue exact;
      AttributeValue prefix;
    }

    public void addRangeKeyCondition(T matcher, Map<String, Condition> keyConditions) {
      AttributeValue rangeKey = null;
      ComparisonOperator comparisonOperator = ComparisonOperator.EQ;

      if (!hasRangeKey()) {
        // We supply the dummy range-key, just for potential speed
        rangeKey = toAttributeValue(EMPTY_RANGE_KEY);
      } else {
        RangeSpecifier spec = buildRangeKey(matcher);
        if (spec != null) {
          if (spec.exact != null) {
            rangeKey = spec.exact;
          } else if (spec.prefix != null) {
            comparisonOperator = ComparisonOperator.BEGINS_WITH;
            rangeKey = spec.prefix;
          } else {
            throw new IllegalStateException();
          }
        }
      }

      if (rangeKey != null) {
        keyConditions.put(FIELD_RANGE_KEY, new Condition().withComparisonOperator(comparisonOperator)
            .withAttributeValueList(rangeKey));
      }
    }

    public RangeSpecifier buildRangeKey(T item) {
      try {
        boolean empty = true;
        boolean complete = true;

        Output output = ByteString.newOutput();

        Message.Builder key = newBuilder();
        for (AttributeMapping rangeKeyField : rangeKeyFields) {
          if (item.hasField(rangeKeyField.field)) {
            Object value = item.getField(rangeKeyField.field);
            key.setField(rangeKeyField.field, value);
            empty = false;
          } else {
            complete = false;
            break;
          }
        }

        if (empty) {
          return null;
        } else {
          key.buildPartial().writeTo(output);
          RangeSpecifier specifier = new RangeSpecifier();
          if (complete) {
            specifier.exact = toAttributeValue(output.toByteString());
          } else {
            specifier.prefix = toAttributeValue(output.toByteString());
          }
          return specifier;
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Error building range key", e);
      }
    }

    public Map<String, AttributeValue> buildCompleteKey(T item) {
      Map<String, AttributeValue> key = Maps.newHashMap();
      key.put(FIELD_HASH_KEY, buildHashKey(item));
      AttributeValue rangeKey;
      if (!hasRangeKey()) {
        // Range key is required; add dummy value
        rangeKey = toAttributeValue(EMPTY_RANGE_KEY);
      } else {
        RangeSpecifier rangeSpec = buildRangeKey(item);
        if (rangeSpec == null || rangeSpec.exact == null) {
          throw new IllegalStateException("Range key fields not specified");
        }
        rangeKey = rangeSpec.exact;
      }
      key.put(FIELD_RANGE_KEY, rangeKey);
      return key;
    }

    boolean hasRangeKey() {
      return !this.rangeKeyFields.isEmpty();
    }

    private Message.Builder newBuilder() {
      Message.Builder message = builderPrototype.clone();
      return message;
    }

    public String getDynamoTableName() {
      return this.dynamoTableName;
    }
  }

  @Override
  public <T extends Message> void addMap(DataStore.Mapping<T> builder) throws DataStoreException {
    String dynamoTableName = DEFAULT_DYNAMO_TABLENAME;
    TypeMetadata typeMetadata = findTypeMetadata(builder.defaultInstance.getDescriptorForType());
    DynamoClassMapping<T> mapping = new DynamoClassMapping<T>(dynamoTableName, builder, typeMetadata);

    mappings.put(builder.defaultInstance.getClass(), mapping);
  }

}
