package com.cloudata.datastore;

public class LimitModifier extends Modifier {
  final int limit;

  public LimitModifier(int limit) {
    this.limit = limit;
  }

  public int getLimit() {
    return limit;
  }

}
