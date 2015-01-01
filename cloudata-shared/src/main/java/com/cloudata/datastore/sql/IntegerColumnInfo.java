package com.cloudata.datastore.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

class IntegerColumnInfo extends ColumnInfo {
  public IntegerColumnInfo(FieldDescriptor[] protobufField, String sqlName) {
    super(protobufField, sqlName);
  }

  public void mapFromDb(ResultSet rs, Message.Builder dest) throws SQLException {
    int value = rs.getInt(this.sqlColumnName);
    setField(dest, value);
  }

  @Override
  public SqlColumnSpec buildColumnSpec() {
    return new SqlColumnSpec(sqlColumnName, "int");
  }

}