package com.cloudata.datastore.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

abstract class ColumnInfo {
  final FieldDescriptor[] protobufFields;
  final String sqlColumnName;

  public ColumnInfo(FieldDescriptor[] protobufFields, String sqlName) {
    this.protobufFields = protobufFields;
    this.sqlColumnName = sqlName;
  }

  public abstract void mapFromDb(ResultSet rs, Message.Builder dest) throws SQLException;

  public abstract SqlColumnSpec buildColumnSpec();

  public void mapToStatement(Message src, PreparedStatement dest, int parameter) throws SQLException {
    Object value = getField(src);
    dest.setObject(parameter, value);
  }

  public boolean hasField(Message message) {
    if (protobufFields.length == 1) {
      return message.hasField(protobufFields[0]);
    } else {
      Message current = message;
      for (int i = 0; i < protobufFields.length - 1; i++) {
        if (!current.hasField(protobufFields[i])) {
          return false;
        }
        current = (Message) current.getField(protobufFields[i]);
      }
      return current.hasField(protobufFields[protobufFields.length - 1]);
    }
  }

  protected void setField(Message.Builder dest, Object value) {
    if (protobufFields.length == 1) {
      dest.setField(protobufFields[0], value);
    } else {
      Message.Builder current = dest;
      for (int i = 0; i < protobufFields.length - 1; i++) {
        current = current.getFieldBuilder(protobufFields[i]);
      }
      current.setField(protobufFields[protobufFields.length - 1], value);
    }
  }

  protected Object getField(Message src) {
    if (protobufFields.length == 1) {
      return src.getField(protobufFields[0]);
    } else {
      Message current = src;
      for (int i = 0; i < protobufFields.length - 1; i++) {
        current = (Message) current.getField(protobufFields[i]);
      }
      return current.getField(protobufFields[protobufFields.length - 1]);
    }
  }

  public String getSqlColumnName() {
    return this.sqlColumnName;
  }
}
