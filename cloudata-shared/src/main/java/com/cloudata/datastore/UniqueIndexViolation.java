package com.cloudata.datastore;

public class UniqueIndexViolation extends DataStoreException {
  public UniqueIndexViolation(String message) {
    super(message);
  }

  private static final long serialVersionUID = 1L;

}
