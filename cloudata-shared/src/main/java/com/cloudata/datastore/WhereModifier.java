package com.cloudata.datastore;

import com.google.protobuf.Message;

public class WhereModifier<T extends Message> extends Modifier {

  final T matcher;

  public WhereModifier(T matcher) {
    this.matcher = matcher;
  }

  public T getMatcher() {
    return matcher;
  }

  public static <T extends Message> WhereModifier<T> create(T matcher) {
    return new WhereModifier<T>(matcher);
  }

  @Override
  public String toString() {
    return "WhereModifier [" + matcher + "]";
  }
  
  
}
