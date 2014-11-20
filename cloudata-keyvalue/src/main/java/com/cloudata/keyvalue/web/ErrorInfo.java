package com.cloudata.keyvalue.web;

public class ErrorInfo {
  final String message;

  public ErrorInfo(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

}
