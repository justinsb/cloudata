package com.cloudata.config;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

public class StaticConfiguration extends Configuration {
  final Map<String, String> config;

  public StaticConfiguration(Map<String, String> config) {
    this.config = Maps.newHashMap(config);
  }

  @Override
  protected String find(String key) {
    return this.config.get(key);
  }

  @Override
  public Iterable<Entry<String, String>> entrySet() {
    return this.config.entrySet();
  }

}
