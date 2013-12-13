// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/main/proto/KeyValueProto.proto

package com.cloudata.keyvalue;

public final class KeyValueProto {
  private KeyValueProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  /**
   * Protobuf enum {@code KvAction}
   */
  public enum KvAction
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>SET = 1;</code>
     */
    SET(0, 1),
    /**
     * <code>DELETE = 2;</code>
     */
    DELETE(1, 2),
    /**
     * <code>INCREMENT = 3;</code>
     */
    INCREMENT(2, 3),
    ;

    /**
     * <code>SET = 1;</code>
     */
    public static final int SET_VALUE = 1;
    /**
     * <code>DELETE = 2;</code>
     */
    public static final int DELETE_VALUE = 2;
    /**
     * <code>INCREMENT = 3;</code>
     */
    public static final int INCREMENT_VALUE = 3;


    public final int getNumber() { return value; }

    public static KvAction valueOf(int value) {
      switch (value) {
        case 1: return SET;
        case 2: return DELETE;
        case 3: return INCREMENT;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<KvAction>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<KvAction>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<KvAction>() {
            public KvAction findValueByNumber(int number) {
              return KvAction.valueOf(number);
            }
          };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return com.cloudata.keyvalue.KeyValueProto.getDescriptor().getEnumTypes().get(0);
    }

    private static final KvAction[] VALUES = values();

    public static KvAction valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }

    private final int index;
    private final int value;

    private KvAction(int index, int value) {
      this.index = index;
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:KvAction)
  }

  public interface KvEntryOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required uint64 store_id = 1;
    /**
     * <code>required uint64 store_id = 1;</code>
     */
    boolean hasStoreId();
    /**
     * <code>required uint64 store_id = 1;</code>
     */
    long getStoreId();

    // required .KvAction action = 2;
    /**
     * <code>required .KvAction action = 2;</code>
     */
    boolean hasAction();
    /**
     * <code>required .KvAction action = 2;</code>
     */
    com.cloudata.keyvalue.KeyValueProto.KvAction getAction();

    // optional bytes key = 3;
    /**
     * <code>optional bytes key = 3;</code>
     */
    boolean hasKey();
    /**
     * <code>optional bytes key = 3;</code>
     */
    com.google.protobuf.ByteString getKey();

    // optional bytes value = 4;
    /**
     * <code>optional bytes value = 4;</code>
     */
    boolean hasValue();
    /**
     * <code>optional bytes value = 4;</code>
     */
    com.google.protobuf.ByteString getValue();

    // optional int64 increment_by = 5;
    /**
     * <code>optional int64 increment_by = 5;</code>
     */
    boolean hasIncrementBy();
    /**
     * <code>optional int64 increment_by = 5;</code>
     */
    long getIncrementBy();
  }
  /**
   * Protobuf type {@code KvEntry}
   */
  public static final class KvEntry extends
      com.google.protobuf.GeneratedMessage
      implements KvEntryOrBuilder {
    // Use KvEntry.newBuilder() to construct.
    private KvEntry(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private KvEntry(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final KvEntry defaultInstance;
    public static KvEntry getDefaultInstance() {
      return defaultInstance;
    }

    public KvEntry getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private KvEntry(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              storeId_ = input.readUInt64();
              break;
            }
            case 16: {
              int rawValue = input.readEnum();
              com.cloudata.keyvalue.KeyValueProto.KvAction value = com.cloudata.keyvalue.KeyValueProto.KvAction.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(2, rawValue);
              } else {
                bitField0_ |= 0x00000002;
                action_ = value;
              }
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              key_ = input.readBytes();
              break;
            }
            case 34: {
              bitField0_ |= 0x00000008;
              value_ = input.readBytes();
              break;
            }
            case 40: {
              bitField0_ |= 0x00000010;
              incrementBy_ = input.readInt64();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.cloudata.keyvalue.KeyValueProto.internal_static_KvEntry_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.cloudata.keyvalue.KeyValueProto.internal_static_KvEntry_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.cloudata.keyvalue.KeyValueProto.KvEntry.class, com.cloudata.keyvalue.KeyValueProto.KvEntry.Builder.class);
    }

    public static com.google.protobuf.Parser<KvEntry> PARSER =
        new com.google.protobuf.AbstractParser<KvEntry>() {
      public KvEntry parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new KvEntry(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<KvEntry> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required uint64 store_id = 1;
    public static final int STORE_ID_FIELD_NUMBER = 1;
    private long storeId_;
    /**
     * <code>required uint64 store_id = 1;</code>
     */
    public boolean hasStoreId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required uint64 store_id = 1;</code>
     */
    public long getStoreId() {
      return storeId_;
    }

    // required .KvAction action = 2;
    public static final int ACTION_FIELD_NUMBER = 2;
    private com.cloudata.keyvalue.KeyValueProto.KvAction action_;
    /**
     * <code>required .KvAction action = 2;</code>
     */
    public boolean hasAction() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required .KvAction action = 2;</code>
     */
    public com.cloudata.keyvalue.KeyValueProto.KvAction getAction() {
      return action_;
    }

    // optional bytes key = 3;
    public static final int KEY_FIELD_NUMBER = 3;
    private com.google.protobuf.ByteString key_;
    /**
     * <code>optional bytes key = 3;</code>
     */
    public boolean hasKey() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional bytes key = 3;</code>
     */
    public com.google.protobuf.ByteString getKey() {
      return key_;
    }

    // optional bytes value = 4;
    public static final int VALUE_FIELD_NUMBER = 4;
    private com.google.protobuf.ByteString value_;
    /**
     * <code>optional bytes value = 4;</code>
     */
    public boolean hasValue() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional bytes value = 4;</code>
     */
    public com.google.protobuf.ByteString getValue() {
      return value_;
    }

    // optional int64 increment_by = 5;
    public static final int INCREMENT_BY_FIELD_NUMBER = 5;
    private long incrementBy_;
    /**
     * <code>optional int64 increment_by = 5;</code>
     */
    public boolean hasIncrementBy() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional int64 increment_by = 5;</code>
     */
    public long getIncrementBy() {
      return incrementBy_;
    }

    private void initFields() {
      storeId_ = 0L;
      action_ = com.cloudata.keyvalue.KeyValueProto.KvAction.SET;
      key_ = com.google.protobuf.ByteString.EMPTY;
      value_ = com.google.protobuf.ByteString.EMPTY;
      incrementBy_ = 0L;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasStoreId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasAction()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeUInt64(1, storeId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeEnum(2, action_.getNumber());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, key_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeBytes(4, value_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        output.writeInt64(5, incrementBy_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeUInt64Size(1, storeId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(2, action_.getNumber());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, key_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(4, value_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(5, incrementBy_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.cloudata.keyvalue.KeyValueProto.KvEntry parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.cloudata.keyvalue.KeyValueProto.KvEntry prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code KvEntry}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.cloudata.keyvalue.KeyValueProto.KvEntryOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.cloudata.keyvalue.KeyValueProto.internal_static_KvEntry_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.cloudata.keyvalue.KeyValueProto.internal_static_KvEntry_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.cloudata.keyvalue.KeyValueProto.KvEntry.class, com.cloudata.keyvalue.KeyValueProto.KvEntry.Builder.class);
      }

      // Construct using com.cloudata.keyvalue.KeyValueProto.KvEntry.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        storeId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        action_ = com.cloudata.keyvalue.KeyValueProto.KvAction.SET;
        bitField0_ = (bitField0_ & ~0x00000002);
        key_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000004);
        value_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000008);
        incrementBy_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000010);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.cloudata.keyvalue.KeyValueProto.internal_static_KvEntry_descriptor;
      }

      public com.cloudata.keyvalue.KeyValueProto.KvEntry getDefaultInstanceForType() {
        return com.cloudata.keyvalue.KeyValueProto.KvEntry.getDefaultInstance();
      }

      public com.cloudata.keyvalue.KeyValueProto.KvEntry build() {
        com.cloudata.keyvalue.KeyValueProto.KvEntry result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.cloudata.keyvalue.KeyValueProto.KvEntry buildPartial() {
        com.cloudata.keyvalue.KeyValueProto.KvEntry result = new com.cloudata.keyvalue.KeyValueProto.KvEntry(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.storeId_ = storeId_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.action_ = action_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.key_ = key_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.value_ = value_;
        if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
          to_bitField0_ |= 0x00000010;
        }
        result.incrementBy_ = incrementBy_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.cloudata.keyvalue.KeyValueProto.KvEntry) {
          return mergeFrom((com.cloudata.keyvalue.KeyValueProto.KvEntry)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.cloudata.keyvalue.KeyValueProto.KvEntry other) {
        if (other == com.cloudata.keyvalue.KeyValueProto.KvEntry.getDefaultInstance()) return this;
        if (other.hasStoreId()) {
          setStoreId(other.getStoreId());
        }
        if (other.hasAction()) {
          setAction(other.getAction());
        }
        if (other.hasKey()) {
          setKey(other.getKey());
        }
        if (other.hasValue()) {
          setValue(other.getValue());
        }
        if (other.hasIncrementBy()) {
          setIncrementBy(other.getIncrementBy());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasStoreId()) {
          
          return false;
        }
        if (!hasAction()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.cloudata.keyvalue.KeyValueProto.KvEntry parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.cloudata.keyvalue.KeyValueProto.KvEntry) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required uint64 store_id = 1;
      private long storeId_ ;
      /**
       * <code>required uint64 store_id = 1;</code>
       */
      public boolean hasStoreId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required uint64 store_id = 1;</code>
       */
      public long getStoreId() {
        return storeId_;
      }
      /**
       * <code>required uint64 store_id = 1;</code>
       */
      public Builder setStoreId(long value) {
        bitField0_ |= 0x00000001;
        storeId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required uint64 store_id = 1;</code>
       */
      public Builder clearStoreId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        storeId_ = 0L;
        onChanged();
        return this;
      }

      // required .KvAction action = 2;
      private com.cloudata.keyvalue.KeyValueProto.KvAction action_ = com.cloudata.keyvalue.KeyValueProto.KvAction.SET;
      /**
       * <code>required .KvAction action = 2;</code>
       */
      public boolean hasAction() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required .KvAction action = 2;</code>
       */
      public com.cloudata.keyvalue.KeyValueProto.KvAction getAction() {
        return action_;
      }
      /**
       * <code>required .KvAction action = 2;</code>
       */
      public Builder setAction(com.cloudata.keyvalue.KeyValueProto.KvAction value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000002;
        action_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required .KvAction action = 2;</code>
       */
      public Builder clearAction() {
        bitField0_ = (bitField0_ & ~0x00000002);
        action_ = com.cloudata.keyvalue.KeyValueProto.KvAction.SET;
        onChanged();
        return this;
      }

      // optional bytes key = 3;
      private com.google.protobuf.ByteString key_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes key = 3;</code>
       */
      public boolean hasKey() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional bytes key = 3;</code>
       */
      public com.google.protobuf.ByteString getKey() {
        return key_;
      }
      /**
       * <code>optional bytes key = 3;</code>
       */
      public Builder setKey(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        key_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes key = 3;</code>
       */
      public Builder clearKey() {
        bitField0_ = (bitField0_ & ~0x00000004);
        key_ = getDefaultInstance().getKey();
        onChanged();
        return this;
      }

      // optional bytes value = 4;
      private com.google.protobuf.ByteString value_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes value = 4;</code>
       */
      public boolean hasValue() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>optional bytes value = 4;</code>
       */
      public com.google.protobuf.ByteString getValue() {
        return value_;
      }
      /**
       * <code>optional bytes value = 4;</code>
       */
      public Builder setValue(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000008;
        value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes value = 4;</code>
       */
      public Builder clearValue() {
        bitField0_ = (bitField0_ & ~0x00000008);
        value_ = getDefaultInstance().getValue();
        onChanged();
        return this;
      }

      // optional int64 increment_by = 5;
      private long incrementBy_ ;
      /**
       * <code>optional int64 increment_by = 5;</code>
       */
      public boolean hasIncrementBy() {
        return ((bitField0_ & 0x00000010) == 0x00000010);
      }
      /**
       * <code>optional int64 increment_by = 5;</code>
       */
      public long getIncrementBy() {
        return incrementBy_;
      }
      /**
       * <code>optional int64 increment_by = 5;</code>
       */
      public Builder setIncrementBy(long value) {
        bitField0_ |= 0x00000010;
        incrementBy_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 increment_by = 5;</code>
       */
      public Builder clearIncrementBy() {
        bitField0_ = (bitField0_ & ~0x00000010);
        incrementBy_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:KvEntry)
    }

    static {
      defaultInstance = new KvEntry(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:KvEntry)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_KvEntry_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_KvEntry_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\"src/main/proto/KeyValueProto.proto\"h\n\007" +
      "KvEntry\022\020\n\010store_id\030\001 \002(\004\022\031\n\006action\030\002 \002(" +
      "\0162\t.KvAction\022\013\n\003key\030\003 \001(\014\022\r\n\005value\030\004 \001(\014" +
      "\022\024\n\014increment_by\030\005 \001(\003*.\n\010KvAction\022\007\n\003SE" +
      "T\020\001\022\n\n\006DELETE\020\002\022\r\n\tINCREMENT\020\003B\027\n\025com.cl" +
      "oudata.keyvalue"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_KvEntry_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_KvEntry_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_KvEntry_descriptor,
              new java.lang.String[] { "StoreId", "Action", "Key", "Value", "IncrementBy", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
