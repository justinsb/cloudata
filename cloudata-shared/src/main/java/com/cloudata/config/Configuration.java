package com.cloudata.config;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public abstract class Configuration {

  private static final Logger log = LoggerFactory.getLogger(Configuration.class);

  public String get(String key, String defaultValue) {
    String v = find(key);
    if (v == null) {
      return defaultValue;
    }
    return v;
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
      String accessKey = get("AWS_ACCESS_KEY_ID", "");
      if (!Strings.isNullOrEmpty(accessKey)) {
        String secretKey = get("AWS_SECRET_KEY", "");
        if (Strings.isNullOrEmpty(secretKey)) {
          throw new IllegalStateException("AWS_SECRET_KEY is not set");
        }
        awsCredentials = new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
      } else {
        log.info("AWS_ACCESS_KEY_ID not set; will try using instance profile");
        InstanceProfileCredentialsProvider instanceProfileCredentialsProvider = new InstanceProfileCredentialsProvider();
        try {
          instanceProfileCredentialsProvider.getCredentials();
          awsCredentials = instanceProfileCredentialsProvider;
        } catch (AmazonClientException e) {
          log.warn("Failed to get credentials from instance profile", e);
        }
      }

      if (awsCredentials == null) {
        throw new IllegalStateException(
            "AWS_ACCESS_KEY_ID is not set, and failed to get credentials from instance profile");
      }
    }
    return awsCredentials;
  }
}
