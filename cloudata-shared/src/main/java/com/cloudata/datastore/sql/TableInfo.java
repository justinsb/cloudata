package com.cloudata.datastore.sql;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.datastore.DataStoreException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

public class TableInfo<T extends Message> {

  private static final Logger log = LoggerFactory.getLogger(TableInfo.class);

  final String sqlTableName;

  final List<ColumnInfo> columns;
  final List<ColumnInfo> keyColumns;
  final Message.Builder builderPrototype;

  private TableInfo(T instance, String tableName, List<ColumnInfo> columns, List<ColumnInfo> keyColumns) {
    this.sqlTableName = tableName;
    this.builderPrototype = instance.newBuilderForType();
    this.columns = ImmutableList.copyOf(columns);
    this.keyColumns = ImmutableList.copyOf(keyColumns);
  }

  public static <T extends Message> TableInfo<T> build(Class<T> messageType, Connection connection)
      throws DataStoreException {
    T instance;
    try {
      Method getDefaultInstance = messageType.getMethod("getDefaultInstance");
      if (getDefaultInstance == null) {
        throw new IllegalStateException("getDefaultInstance method not found");
      }
      instance = (T) getDefaultInstance.invoke(null);
    } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException | SecurityException e) {
      throw new IllegalStateException("Failed to build object of type " + messageType, e);
    }

    Descriptor descriptor = instance.getDescriptorForType();

    String sqlTableName = toSql(descriptor.getName());
    List<String> pkColumns = getPkColumns(connection, sqlTableName);

    List<ColumnInfo> columns = Lists.newArrayList();
    buildColumns(columns, descriptor, Collections.<FieldDescriptor> emptyList(), "");

    List<ColumnInfo> keyColumns = Lists.newArrayList();
    for (ColumnInfo column : columns) {
      if (pkColumns.contains(column.getSqlColumnName())) {
        keyColumns.add(column);
      }
    }

    TableInfo tableInfo = new TableInfo(instance, sqlTableName, columns, keyColumns);
    log.info("Schema for table: " + tableInfo.getCreateStatement());
    return tableInfo;
  }

  private String getCreateStatement() {
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE ");
    sql.append(sqlTableName);
    sql.append("(");
    sql.append(Joiner.on(",").join(Iterables.transform(columns, column -> column.buildColumnSpec().toSql())));
    sql.append(")");
    return sql.toString();
  }

  private static void buildColumns(List<ColumnInfo> columns, Descriptor descriptor, List<FieldDescriptor> fieldPrefix,
      String sqlPrefix) {

    for (FieldDescriptor field : descriptor.getFields()) {
      String sqlName = sqlPrefix + toSql(field.getName());

      switch (field.getType()) {
      case STRING:
        columns.add(new StringColumnInfo(concat(fieldPrefix, field), sqlName));
        break;

      case BYTES:
        columns.add(new BytesColumnInfo(concat(fieldPrefix, field), sqlName));
        break;

      case INT32:
        columns.add(new IntegerColumnInfo(concat(fieldPrefix, field), sqlName));
        break;

      case MESSAGE:
        Descriptor messageType = field.getMessageType();
        List<FieldDescriptor> childFieldPrefix = Lists.newArrayList(fieldPrefix);
        childFieldPrefix.add(field);
        buildColumns(columns, messageType, childFieldPrefix, sqlName + "_");
        break;

      default:
        throw new IllegalStateException("Unhandled protobuf type: " + field.getType());
      }
    }

  }

  private static FieldDescriptor[] concat(List<FieldDescriptor> fieldPrefix, FieldDescriptor field) {
    int n = fieldPrefix.size() + 1;
    FieldDescriptor[] array = new FieldDescriptor[n];
    fieldPrefix.toArray(array);
    array[n - 1] = field;
    return array;
  }

  private static List<String> getPkColumns(Connection connection, String sqlTableName) throws DataStoreException {

    try {
      String catalog = connection.getCatalog();
      String schema = "";// connection.getSchema();

      ResultSet rs = connection.getMetaData().getPrimaryKeys(catalog, schema, sqlTableName);
      List<String> columns = Lists.newArrayList();

      while (rs.next()) {
        // TABLE_CAT String => table catalog (may be null)
        // TABLE_SCHEM String => table schema (may be null)
        // TABLE_NAME String => table name
        // COLUMN_NAME String => column name
        // KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary
        // key, a value of 2 would represent the second column within the primary key).
        // PK_NAME String => primary key name (may be null)
        String columnName = rs.getString("COLUMN_NAME");
        short keySequence = rs.getShort("KEY_SEQ");
        while (columns.size() < keySequence) {
          columns.add(null);
        }
        columns.set(keySequence - 1, columnName);
      }
      return columns;
    } catch (SQLException e) {
      throw new DataStoreException("Error fetching primary key for table", e);
    }

    // boolean unique = true;
    // ResultSet rs = connection.getMetaData().getIndexInfo(catalog, schema, sqlTableName, unique, false);
    //
    // Map<String, List<String>> indexes = Maps.newHashMap();
    //
    // while (rs.next()) {
    // String indexName = rs.getString("INDEX_NAME");
    // short indexType = rs.getShort("TYPE");
    // if (indexType == DatabaseMetaData.tableIndexStatistic) {
    // continue;
    // }
    // short ordinalPosition = rs.getShort("ORDINAL_POSITION");
    // String columnName = rs.getString("COLUMN_NAME");
    //
    // }

  }

  public static String toSql(String name) {
    // TODO: map to underscores?
    return name.toLowerCase();
  }

  public T mapFromDb(ResultSet rs) throws SQLException {
    Message.Builder dest = newBuilder();
    for (ColumnInfo column : columns) {
      column.mapFromDb(rs, dest);
    }
    return (T) dest.build();
  }

  private Message.Builder newBuilder() {
    Message.Builder message = builderPrototype.clone();
    // try {
    // message = messageClass.newInstance();
    // } catch (InstantiationException | IllegalAccessException e) {
    // throw new IllegalStateException("Error building message", e);
    // }
    return message;
  }

  public List<ColumnInfo> getColumns() {
    return columns;
  }

  public List<ColumnInfo> getKeyColumns() {
    return keyColumns;
  }

  public String getSqlTableName() {
    return this.sqlTableName;
  }

  public boolean isKeyColumn(ColumnInfo column) {
    return keyColumns.contains(column);
  }
}
