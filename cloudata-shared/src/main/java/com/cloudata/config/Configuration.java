package com.cloudata.config;

import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public abstract class Configuration {

  public String get(String key, String defaultValue) {
    String v = find(key);
    if (v == null) {
      return defaultValue;
    }
    return (String) v;
  }

  protected abstract String find(String key);

  public static Configuration build() {
    Map<String, String> envMap = Maps.newHashMap();
    for (Entry<String, String> entry : System.getenv().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      envMap.put(key, value);
    }
    Configuration env = new StaticConfiguration(envMap);
    return env;
  }

  public abstract Iterable<Map.Entry<String, String>> entrySet();

  AWSCredentialsProvider awsCredentials;
  public AWSCredentialsProvider getAwsCredentials() {
    if (this.awsCredentials == null) {
      // TODO: Fallback to instance role
      String accessKey = get("AWS_ACCESS_KEY_ID", "");
      if (Strings.isNullOrEmpty(accessKey)) {
        throw new IllegalStateException("AWS_ACCESS_KEY_ID is not set");
      }
      String secretKey = get("AWS_SECRET_KEY", "");
      if (Strings.isNullOrEmpty(secretKey)) {
        throw new IllegalStateException("AWS_SECRET_KEY is not set");
      }

      awsCredentials = new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
    }
    return awsCredentials;
  }
}
