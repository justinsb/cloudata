package com.cloudata.datastore.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

class StringColumnInfo extends ColumnInfo {
  public StringColumnInfo(FieldDescriptor[] protobufField, String sqlName) {
    super(protobufField, sqlName);
  }

  @Override
  public void mapFromDb(ResultSet rs, Message.Builder dest) throws SQLException {
    String value = rs.getString(this.sqlColumnName);
    if (value != null) {
      setField(dest, value);
    }
  }

  @Override
  public SqlColumnSpec buildColumnSpec() {
    return new SqlColumnSpec(sqlColumnName, "varchar");
  }

}
