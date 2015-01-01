package com.cloudata.datastore.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.Modifier;
import com.cloudata.datastore.WhereModifier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;

@Singleton
public class SqlDataStore implements DataStore {

  private static final Logger log = LoggerFactory.getLogger(SqlDataStore.class);

  final DataSource poolingDataSource;

  public SqlDataStore(DataSource poolingDataSource) {
    this.poolingDataSource = poolingDataSource;
  }

  final Map<Class<?>, TableInfo> tables = Maps.newHashMap();

  <T extends Message> TableInfo<T> getTableInfo(Class<T> messageType) throws DataStoreException {
    TableInfo tableInfo = tables.get(messageType);
    if (tableInfo == null) {
      try (Connection connection = poolingDataSource.getConnection()) {
        tableInfo = TableInfo.build(messageType, connection);
      } catch (SQLException e) {
        throw new DataStoreException("Error build mapping for table", e);
      }
      tables.put(messageType, tableInfo);
    }
    return tableInfo;
  }

  @Override
  public <T extends Message> Iterable<T> find(T matcher, Modifier... modifiers) throws DataStoreException {
    List<T> results = findMatching(matcher, modifiers);
    return results;
  }

  @Override
  public <T extends Message> T findOne(T matcher, Modifier... modifiers) throws DataStoreException {
    List<T> results = findMatching(matcher, modifiers);

    if (results.size() == 0) {
      return null;
    }
    if (results.size() != 1) {
      throw new DataStoreException("Found multiple results; expecting exactly one");
    }
    return results.get(0);
  }

  private <T extends Message> List<T> findMatching(T matcher, Modifier... modifiers) throws DataStoreException {
    TableInfo<T> tableInfo = getTableInfo((Class<T>) matcher.getClass());

    String sql;
    {
      StringBuilder sb = new StringBuilder();
      sb.append("SELECT * FROM ");
      sb.append(tableInfo.getSqlTableName());
      int n = 0;
      for (ColumnInfo column : tableInfo.getColumns()) {
        if (!column.hasField(matcher)) {
          continue;
        }
        if (n == 0) {
          sb.append(" WHERE ");
        } else {
          sb.append(", ");
        }
        sb.append(column.getSqlColumnName());
        sb.append("=?");
        n++;
      }
      for (Modifier modifier : modifiers) {
        throw new UnsupportedOperationException();
      }
      sql = sb.toString();
    }

    try (Connection connection = getConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        int n = 0;
        for (ColumnInfo column : tableInfo.getColumns()) {
          if (!column.hasField(matcher)) {
            continue;
          }
          n++;
          column.mapToStatement(matcher, preparedStatement, n);
        }

        log.debug("Executing SQL: {}", sql);

        ResultSet rs = preparedStatement.executeQuery();
        List<T> results = Lists.newArrayList();
        while (rs.next()) {
          T result = tableInfo.mapFromDb(rs);
          results.add(result);
        }
        return results;
      }
    } catch (SQLException e) {
      throw new DataStoreException("Error querying database", e);
    }
  }

  @Override
  public <T extends Message> void insert(T data, Modifier... modifiers) throws DataStoreException {
    TableInfo<T> tableInfo = getTableInfo((Class<T>) data.getClass());

    String sql;
    {
      StringBuilder sb = new StringBuilder();
      sb.append("INSERT INTO ");
      sb.append(tableInfo.getSqlTableName());
      sb.append(" (");
      int n = 0;
      for (ColumnInfo column : tableInfo.getColumns()) {
        if (!column.hasField(data)) {
          continue;
        }
        if (n != 0) {
          sb.append(", ");
        }
        sb.append(column.getSqlColumnName());
        n++;
      }
      sb.append(") VALUES (");
      n = 0;
      for (ColumnInfo column : tableInfo.getColumns()) {
        if (!column.hasField(data)) {
          continue;
        }
        if (n != 0) {
          sb.append(", ");
        }
        sb.append("?");
        n++;
      }
      sb.append(")");

      for (Modifier modifier : modifiers) {
        if (modifier instanceof WhereModifier<?>) {
          throw new UnsupportedOperationException();
        } else {
          throw new IllegalStateException("Unhandler modifier: " + modifier.getClass().getSimpleName());
        }
      }
      sql = sb.toString();
    }

    try (Connection connection = getConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        int n = 0;
        for (ColumnInfo column : tableInfo.getColumns()) {
          if (!column.hasField(data)) {
            continue;
          }
          n++;
          column.mapToStatement(data, preparedStatement, n);
        }

        int rowCount = preparedStatement.executeUpdate();
        if (rowCount > 1) {
          throw new IllegalStateException("Multiple rows inserted");
        }

        if (rowCount == 0) {
          throw new IllegalStateException("Row not inserted");
        }
        return;
      }
    } catch (SQLException e) {
      throw new DataStoreException("Error querying database", e);
    }
  }

  @Override
  public <T extends Message> boolean update(T data, Modifier... modifiers) throws DataStoreException {
    TableInfo<T> tableInfo = getTableInfo((Class<T>) data.getClass());

    String sql;
    {
      StringBuilder sb = new StringBuilder();
      sb.append("UPDATE ");
      sb.append(tableInfo.getSqlTableName());
      sb.append(" SET ");
      int n = 0;
      for (ColumnInfo column : tableInfo.getColumns()) {
        if (tableInfo.isKeyColumn(column)) {
          continue;
        }
        if (!column.hasField(data)) {
          continue;
        }
        if (n != 0) {
          sb.append(", ");
        }
        sb.append(column.getSqlColumnName());
        sb.append("=?");
        n++;
      }
      sb.append(" WHERE ");
      n = 0;
      for (ColumnInfo column : tableInfo.getKeyColumns()) {
        if (!column.hasField(data)) {
          throw new IllegalStateException("Key column not specified: " + column);
        }
        if (n != 0) {
          sb.append(", ");
        }
        sb.append(column.getSqlColumnName());
        sb.append("=?");
        n++;
      }

      for (Modifier modifier : modifiers) {
        if (modifier instanceof WhereModifier<?>) {
          throw new UnsupportedOperationException();
        } else {
          throw new IllegalStateException("Unhandler modifier: " + modifier.getClass().getSimpleName());
        }
      }
      sql = sb.toString();
    }

    try (Connection connection = getConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        int n = 0;
        for (ColumnInfo column : tableInfo.getColumns()) {
          if (tableInfo.isKeyColumn(column)) {
            continue;
          }
          if (!column.hasField(data)) {
            continue;
          }
          n++;
          column.mapToStatement(data, preparedStatement, n);
        }
        for (ColumnInfo column : tableInfo.getKeyColumns()) {
          if (!column.hasField(data)) {
            throw new IllegalStateException();
          }
          n++;
          column.mapToStatement(data, preparedStatement, n);
        }

        int rowCount = preparedStatement.executeUpdate();
        if (rowCount > 1) {
          throw new IllegalStateException("Multiple rows inserted");
        }
        return rowCount != 0;
      }
    } catch (SQLException e) {
      throw new DataStoreException("Error querying database", e);
    }
  }

  @Override
  public <T extends Message> boolean delete(T matcher, Modifier... modifiers) throws DataStoreException {
    TableInfo<T> tableInfo = getTableInfo((Class<T>) matcher.getClass());

    String sql;
    {
      StringBuilder sb = new StringBuilder();
      sb.append("DELETE FROM ");
      sb.append(tableInfo.getSqlTableName());
      int n = 0;
      for (ColumnInfo column : tableInfo.getKeyColumns()) {
        if (!column.hasField(matcher)) {
          throw new IllegalStateException("Key column not specified: " + column.getSqlColumnName());
        }
        if (n == 0) {
          sb.append(" WHERE ");
        } else {
          sb.append(", ");
        }
        sb.append(column.getSqlColumnName());
        sb.append("=?");
        n++;
      }
      for (Modifier modifier : modifiers) {
        if (modifier instanceof WhereModifier<?>) {
          throw new UnsupportedOperationException();
        } else {
          throw new IllegalStateException("Unhandler modifier: " + modifier.getClass().getSimpleName());
        }
      }
      sql = sb.toString();
    }

    try (Connection connection = getConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        int n = 0;
        for (ColumnInfo column : tableInfo.getKeyColumns()) {
          n++;
          column.mapToStatement(matcher, preparedStatement, n);
        }

        int rowCount = preparedStatement.executeUpdate();
        if (rowCount > 1) {
          throw new IllegalStateException("Multiple rows changed, despite primary key being specified");
        }
        return rowCount != 0;
      }
    } catch (SQLException e) {
      throw new DataStoreException("Error querying database", e);
    }
  }

  private Connection getConnection() throws DataStoreException {
    try {
      return poolingDataSource.getConnection();
    } catch (SQLException e) {
      throw new DataStoreException("Error establishing database connection", e);
    }
  }
}
