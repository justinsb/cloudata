package com.cloudata.datastore;

import java.io.IOException;

public class DataStoreException extends IOException {
  public DataStoreException(String message) {
    super(message);
  }

  public DataStoreException(String message, Exception cause) {
    super(message, cause);
  }

  private static final long serialVersionUID = 1L;

}
