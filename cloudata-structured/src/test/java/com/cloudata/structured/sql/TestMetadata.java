package com.cloudata.structured.sql;
//package com.cloudata.structured;
//
//import java.util.List;
//import java.util.Map;
//
//import com.facebook.presto.metadata.FunctionHandle;
//import com.facebook.presto.metadata.FunctionInfo;
//import com.facebook.presto.metadata.Metadata;
//import com.facebook.presto.metadata.QualifiedTableName;
//import com.facebook.presto.metadata.QualifiedTablePrefix;
//import com.facebook.presto.metadata.TableMetadata;
//import com.facebook.presto.spi.ColumnHandle;
//import com.facebook.presto.spi.ColumnMetadata;
//import com.facebook.presto.spi.SchemaTableName;
//import com.facebook.presto.spi.TableHandle;
//import com.facebook.presto.sql.analyzer.Type;
//import com.facebook.presto.sql.tree.QualifiedName;
//import com.google.common.base.Optional;
//
//public class TestMetadata implements Metadata {
//
//    @Override
//    public FunctionInfo getFunction(QualifiedName name, List<Type> parameterTypes) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public FunctionInfo getFunction(FunctionHandle handle) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public boolean isAggregationFunction(QualifiedName name) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public List<FunctionInfo> listFunctions() {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public List<String> listSchemaNames(String catalogName) {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public Optional<TableHandle> getTableHandle(QualifiedTableName tableName) {
//        return Optional.of((TableHandle) new MockTableHandle(connectorId, tableName));
//    }
//
//    @Override
//    public TableMetadata getTableMetadata(TableHandle tableHandle) {
//        return ((MockTableHandle) tableHandle).getTableMetadata();
//
//    }
//
//    @Override
//    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix) {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public Optional<ColumnHandle> getColumnHandle(TableHandle tableHandle, String columnName) {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle) {
//        return ((MockTableHandle) tableHandle).getColumnHandles();
//    }
//
//    @Override
//    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle) {
//        return ((MockColumnHandle) columnHandle).getColumnMetadata();
//    }
//
//    @Override
//    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix) {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public TableHandle createTable(String catalogName, TableMetadata tableMetadata) {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public void dropTable(TableHandle tableHandle) {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public String getConnectorId(TableHandle tableHandle) {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public Optional<TableHandle> getTableHandle(String connectorId, SchemaTableName tableName) {
//        throw new UnsupportedOperationException();
//
//    }
//
//    @Override
//    public Map<String, String> getCatalogNames() {
//        throw new UnsupportedOperationException();
//
//    }
//
// }
