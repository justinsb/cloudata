package com.cloudata.datastore;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

public class ComparatorModifier<T extends Message> extends Modifier {
  final FieldDescriptor field;
  final Operator operator;
  final Comparable<T> thresholdValue;

  public static enum Operator {
    LESS_THAN, GREATER_THAN, STARTS_WITH
  }

  public ComparatorModifier(Operator operator, int field, T threshold) {
    this.field = threshold.getDescriptorForType().findFieldByNumber(field);
    if (this.field == null) {
      throw new IllegalArgumentException("Field not found: #" + field);
    }
    this.operator = operator;
    this.thresholdValue = (Comparable) threshold.getField(this.field);
  }

  public static <T extends Message> ComparatorModifier<T> lessThan(T threshold) {
    int field = getFieldIndex(threshold);
    return new ComparatorModifier<T>(Operator.LESS_THAN, field, threshold);
  }

  public static <T extends Message> ComparatorModifier<T> greaterThan(T threshold) {
    int field = getFieldIndex(threshold);
    return new ComparatorModifier<T>(Operator.GREATER_THAN, field, threshold);
  }

  public static <T extends Message> Modifier startsWith(T threshold) {
    int field = getFieldIndex(threshold);
    return new ComparatorModifier<T>(Operator.STARTS_WITH, field, threshold);
  }

  static int getFieldIndex(Message message) {
    int fieldIndex = -1;
    for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
      if (message.hasField(field)) {
        if (fieldIndex != -1) {
          throw new IllegalStateException();
        }
        fieldIndex = field.getIndex();
      }
    }
    if (fieldIndex == -1) {
      throw new IllegalStateException();
    }
    return fieldIndex;
  }

  public boolean matches(T message) {
    Object messageValue = message.getField(field);
    switch (operator) {
    case LESS_THAN:
      if (messageValue == null) {
        return false;
      }
      return ((Comparable) messageValue).compareTo(thresholdValue) < 0;
    default:
      throw new IllegalStateException();
    }
  }

}
