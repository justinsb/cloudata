package com.cloudata.datastore.sql;

public class SqlColumnSpec {
  final String name;
  final String type;

  public SqlColumnSpec(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public String toSql() {
    return name + " " + type;
  }

}
