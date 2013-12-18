package com.cloudata.structured.sql.simple;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.cloudata.btree.Keyspace;
import com.cloudata.structured.sql.RowsetListener;
import com.cloudata.structured.sql.SqlEngine;
import com.cloudata.structured.sql.SqlSession;
import com.cloudata.structured.sql.SqlStatement;
import com.cloudata.structured.sql.provider.CloudataRecordCursor;
import com.cloudata.structured.sql.provider.CloudataRecordCursor.WalkListener;
import com.cloudata.structured.sql.value.ValueHolder;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.JsonObject;

public class SimpleExecutor {

    final SqlEngine sqlEngine;
    final SqlSession sqlSession;
    final SqlStatement sqlStatement;

    final ExecutorService executor;

    public SimpleExecutor(SqlEngine sqlEngine, SqlSession sqlSession, SqlStatement sqlStatement) {
        this.sqlEngine = sqlEngine;
        this.sqlSession = sqlSession;
        this.sqlStatement = sqlStatement;

        this.executor = sqlEngine.getExecutor();
    }

    public void execute(final RowsetListener listener) {

        // QueryId queryId = new QueryId("query0");
        // StageId stageId = new StageId(queryId, "stage0");
        // TaskId taskId = new TaskId(stageId, "task0");
        // TaskContext taskContext = new TaskContext(taskId, executor, sqlSession.getPrestoSession());

        // boolean inputPipeline = false;
        // boolean outputPipeline = true;
        // PipelineContext pipelineContext = new PipelineContext(taskContext, executor, inputPipeline, outputPipeline);
        // DriverContext driverContext = new DriverContext(pipelineContext, executor);

        // int operatorId = 1;
        // String operatorType = getClass().getSimpleName();
        // OperatorContext operatorContext = new OperatorContext(operatorId, operatorType, driverContext, executor);

        SimpleNode simpleNode = sqlStatement.getSimple();
        Preconditions.checkState(simpleNode != null);

        SimpleTableScan tableScan = (SimpleTableScan) simpleNode;

        RecordSet rs = sqlEngine.getRecordSet(tableScan.tableMetadata);
        CloudataRecordCursor cursor = (CloudataRecordCursor) rs.cursor();

        final SimpleColumnExpression[] columns = tableScan.columns;

        List<SimpleExpression> expressionsList = tableScan.expressions;
        final SimpleExpression[] expressions = expressionsList.toArray(new SimpleExpression[expressionsList.size()]);

        final ValueHolder[] expressionValues = new ValueHolder[expressions.length];
        for (int i = 0; i < expressionValues.length; i++) {
            expressionValues[i] = new ValueHolder();
        }
        // final ValueHolder[] columnValues = new ValueHolder[columns.length];
        // for (int i = 0; i < columnValues.length; i++) {
        // columnValues[i] = columns[i].valueHolder;
        // }

        try {
            listener.beginRows();

            ByteBuffer start = null;
            cursor.walk(Keyspace.ZERO, start, new WalkListener() {

                @Override
                public boolean found(ByteBuffer key, JsonObject json) {
                    try {
                        for (int i = 0; i < columns.length; i++) {
                            SimpleColumnExpression column = columns[i];
                            column.update(json);
                        }

                        for (int i = 0; i < expressionValues.length; i++) {
                            expressions[i].evalute(expressionValues[i]);
                        }

                        return listener.gotRow(expressionValues);
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }

            });

            listener.endRows();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
