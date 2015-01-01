package com.cloudata.mq.web;

public class AwsException extends Exception {
  final int code;
  final String key;

  public AwsException(int code, String key) {
    this.code = code;
    this.key = key;
  }

}
