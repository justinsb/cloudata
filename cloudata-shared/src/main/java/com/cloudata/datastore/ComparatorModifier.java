package com.cloudata.datastore;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

public class ComparatorModifier<T extends Message> extends Modifier {
  final FieldDescriptor field;
  final Operator operator;
  final Comparable thresholdValue;

  public static enum Operator {
    LESS_THAN
  }

  public ComparatorModifier(int field, Operator operator, T threshold) {
    this.field = threshold.getDescriptorForType().findFieldByNumber(field);
    if (this.field == null) {
      throw new IllegalArgumentException("Field not found: #" + field);
    }
    this.operator = operator;
    this.thresholdValue = (Comparable) threshold.getField(this.field);

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
