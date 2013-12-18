//package com.cloudata.structured.sql;
//
//import static com.google.common.base.Preconditions.checkArgument;
//import static com.google.common.base.Preconditions.checkNotNull;
//import io.airlift.units.Duration;
//
//import java.io.Closeable;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//
//import javax.annotation.concurrent.GuardedBy;
//import javax.ws.rs.WebApplicationException;
//import javax.ws.rs.core.Response.Status;
//import javax.ws.rs.core.UriInfo;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.facebook.presto.block.BlockCursor;
//import com.facebook.presto.client.Column;
//import com.facebook.presto.client.FailureInfo;
//import com.facebook.presto.client.QueryError;
//import com.facebook.presto.client.QueryResults;
//import com.facebook.presto.client.StageStats;
//import com.facebook.presto.client.StatementStats;
//import com.facebook.presto.execution.BufferInfo;
//import com.facebook.presto.execution.QueryId;
//import com.facebook.presto.execution.QueryInfo;
//import com.facebook.presto.execution.QueryManager;
//import com.facebook.presto.execution.QueryState;
//import com.facebook.presto.execution.QueryStats;
//import com.facebook.presto.execution.StageInfo;
//import com.facebook.presto.execution.StageState;
//import com.facebook.presto.execution.TaskInfo;
//import com.facebook.presto.operator.ExchangeClient;
//import com.facebook.presto.operator.Page;
//import com.facebook.presto.sql.analyzer.Session;
//import com.facebook.presto.tuple.TupleInfo;
//import com.facebook.presto.util.IterableTransformer;
//import com.google.common.base.Preconditions;
//import com.google.common.base.Predicate;
//import com.google.common.collect.AbstractIterator;
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Iterables;
//import com.google.common.collect.Lists;
//
//public class Query implements Closeable {
//
//    private static final Logger log = LoggerFactory.getLogger(Query.class);
//
//    private final QueryManager queryManager;
//    private final QueryId queryId;
//    private final ExchangeClient exchangeClient;
//
//    private final AtomicLong resultId = new AtomicLong();
//
//    @GuardedBy("this")
//    private QueryResults lastResult;
//
//    @GuardedBy("this")
//    private String lastResultPath;
//
//    @GuardedBy("this")
//    private List<Column> columns;
//
//    public Query(Session session, String query, QueryManager queryManager, ExchangeClient exchangeClient) {
//        checkNotNull(session, "session is null");
//        checkNotNull(query, "query is null");
//        checkNotNull(queryManager, "queryManager is null");
//        checkNotNull(exchangeClient, "exchangeClient is null");
//
//        this.queryManager = queryManager;
//
//        QueryInfo queryInfo = queryManager.createQuery(session, query);
//        queryId = queryInfo.getQueryId();
//        this.exchangeClient = exchangeClient;
//    }
//
//    @Override
//    public void close() {
//        queryManager.cancelQuery(queryId);
//    }
//
//    public QueryId getQueryId() {
//        return queryId;
//    }
//
//    public synchronized QueryResults getResults(long token, UriInfo uriInfo, Duration maxWaitTime)
//            throws InterruptedException {
//        // is the a repeated request for the last results?
//        String requestedPath = uriInfo.getAbsolutePath().getPath();
//        if (lastResultPath != null && requestedPath.equals(lastResultPath)) {
//            // tell query manager we are still interested in the query
//            queryManager.getQueryInfo(queryId);
//            return lastResult;
//        }
//
//        if (token < resultId.get()) {
//            throw new WebApplicationException(Status.GONE);
//        }
//
//        // if this is not a request for the next results, return not found
//        if (lastResult.getNextUri() == null || !requestedPath.equals(lastResult.getNextUri().getPath())) {
//            // unknown token
//            throw new WebApplicationException(Status.NOT_FOUND);
//        }
//
//        return getNextResults(uriInfo, maxWaitTime);
//    }
//
//    public synchronized QueryResults getNextResults(UriInfo uriInfo, Duration maxWaitTime) throws InterruptedException {
//        Iterable<List<Object>> data = getData(maxWaitTime);
//
//        // get the query info before returning
//        // force update if query manager is closed
//        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
//
//        // if we have received all of the output data and the query is not marked as done, wait for the query to finish
//        if (exchangeClient.isClosed() && !queryInfo.getState().isDone()) {
//            queryManager.waitForStateChange(queryId, queryInfo.getState(), maxWaitTime);
//            queryInfo = queryManager.getQueryInfo(queryId);
//        }
//
//        // close exchange client if the query has failed
//        if (queryInfo.getState().isDone()) {
//            if (queryInfo.getState() != QueryState.FINISHED) {
//                exchangeClient.close();
//            } else if (queryInfo.getOutputStage() == null) {
//                // For simple executions (e.g. drop table), there will never be an output stage,
//                // so close the exchange as soon as the query is done.
//                exchangeClient.close();
//
//                // this is a hack to suppress the warn message in the client saying that there are no columns.
//                // The reason for this is that the current API definition assumes that everything is a query,
//                // so statements without results produce an error in the client otherwise.
//                //
//                // TODO: add support to the API for non-query statements.
//                columns = ImmutableList.of(new Column("result", "varchar"));
//                data = ImmutableSet.<List<Object>> of(ImmutableList.<Object> of("true"));
//            }
//        }
//
//        // only return a next if the query is not done or there is more data to send (due to buffering)
//        URI nextResultsUri = null;
//        if ((!queryInfo.getState().isDone()) || (!exchangeClient.isClosed())) {
//            nextResultsUri = createNextResultsUri(uriInfo);
//        }
//
//        // first time through, self is null
//        QueryResults queryResults = new QueryResults(queryId.toString(), uriInfo.getRequestUriBuilder()
//                .replaceQuery("").replacePath(queryInfo.getSelf().getPath()).build(),
//                findCancelableLeafStage(queryInfo), nextResultsUri, columns, data, toStatementStats(queryInfo),
//                toQueryError(queryInfo));
//
//        // cache the last results
//        if (lastResult != null) {
//            lastResultPath = lastResult.getNextUri().getPath();
//        } else {
//            lastResultPath = null;
//        }
//        lastResult = queryResults;
//        return queryResults;
//    }
//
//    private synchronized Iterable<List<Object>> getData(Duration maxWait) throws InterruptedException {
//        // wait for query to start
//        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
//        while (maxWait.toMillis() > 1 && !isQueryStarted(queryInfo)) {
//            maxWait = queryManager.waitForStateChange(queryId, queryInfo.getState(), maxWait);
//            queryInfo = queryManager.getQueryInfo(queryId);
//        }
//
//        // if query did not finish starting or does not have output, just return
//        if (!isQueryStarted(queryInfo) || queryInfo.getOutputStage() == null) {
//            return null;
//        }
//
//        if (columns == null) {
//            columns = createColumnsList(queryInfo);
//        }
//
//        updateExchangeClient(queryInfo.getOutputStage());
//
//        ImmutableList.Builder<RowIterable> pages = ImmutableList.builder();
//        // wait up to max wait for data to arrive; then try to return at least DESIRED_RESULT_BYTES
//        int bytes = 0;
//        while (bytes < DESIRED_RESULT_BYTES) {
//            Page page = exchangeClient.getNextPage(maxWait);
//            if (page == null) {
//                break;
//            }
//            bytes += page.getDataSize().toBytes();
//            pages.add(new RowIterable(page));
//
//            // only wait on first call
//            maxWait = new Duration(0, TimeUnit.MILLISECONDS);
//        }
//
//        if (bytes == 0) {
//            return null;
//        }
//
//        return Iterables.concat(pages.build());
//    }
//
//    private static boolean isQueryStarted(QueryInfo queryInfo) {
//        QueryState state = queryInfo.getState();
//        return state != QueryState.QUEUED && queryInfo.getState() != QueryState.PLANNING
//                && queryInfo.getState() != QueryState.STARTING;
//    }
//
//    private synchronized void updateExchangeClient(StageInfo outputStage) {
//        // update the exchange client with any additional locations
//        for (TaskInfo taskInfo : outputStage.getTasks()) {
//            List<BufferInfo> buffers = taskInfo.getOutputBuffers().getBuffers();
//            Preconditions.checkState(buffers.size() == 1, "Expected a single output buffer for task %s, but found %s",
//                    taskInfo.getTaskId(), buffers);
//
//            String bufferId = Iterables.getOnlyElement(buffers).getBufferId();
//            URI uri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath(bufferId).build();
//            exchangeClient.addLocation(uri);
//        }
//        if ((outputStage.getState() != StageState.PLANNED) && (outputStage.getState() != StageState.SCHEDULING)) {
//            exchangeClient.noMoreLocations();
//        }
//    }
//
//    private synchronized URI createNextResultsUri(UriInfo uriInfo) {
//        return uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString())
//                .path(String.valueOf(resultId.incrementAndGet())).replaceQuery("").build();
//    }
//
//    private static List<Column> createColumnsList(QueryInfo queryInfo) {
//        checkNotNull(queryInfo, "queryInfo is null");
//        StageInfo outputStage = queryInfo.getOutputStage();
//        if (outputStage == null) {
//            checkNotNull(outputStage, "outputStage is null");
//        }
//
//        List<String> names = queryInfo.getFieldNames();
//        ArrayList<Type> types = new ArrayList<>();
//        for (TupleInfo tupleInfo : outputStage.getTupleInfos()) {
//            types.addAll(tupleInfo.getTypes());
//        }
//
//        checkArgument(names.size() == types.size(), "names and types size mismatch");
//
//        ImmutableList.Builder<Column> list = ImmutableList.builder();
//        for (int i = 0; i < names.size(); i++) {
//            String name = names.get(i);
//            Type type = types.get(i);
//            switch (type) {
//            case BOOLEAN:
//                list.add(new Column(name, "boolean"));
//                break;
//            case FIXED_INT_64:
//                list.add(new Column(name, "bigint"));
//                break;
//            case DOUBLE:
//                list.add(new Column(name, "double"));
//                break;
//            case VARIABLE_BINARY:
//                list.add(new Column(name, "varchar"));
//                break;
//            default:
//                throw new IllegalArgumentException("unhandled type: " + type);
//            }
//        }
//        return list.build();
//    }
//
//    private static StatementStats toStatementStats(QueryInfo queryInfo) {
//        QueryStats queryStats = queryInfo.getQueryStats();
//
//        return StatementStats.builder().setState(queryInfo.getState().toString()).setScheduled(isScheduled(queryInfo))
//                .setNodes(globalUniqueNodes(queryInfo.getOutputStage()).size())
//                .setTotalSplits(queryStats.getTotalDrivers()).setQueuedSplits(queryStats.getQueuedDrivers())
//                .setRunningSplits(queryStats.getRunningDrivers()).setCompletedSplits(queryStats.getCompletedDrivers())
//                .setUserTimeMillis(queryStats.getTotalUserTime().toMillis())
//                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
//                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
//                .setProcessedRows(queryStats.getRawInputPositions())
//                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
//                .setRootStage(toStageStats(queryInfo.getOutputStage())).build();
//    }
//
//    private static StageStats toStageStats(StageInfo stageInfo) {
//        if (stageInfo == null) {
//            return null;
//        }
//
//        com.facebook.presto.execution.StageStats stageStats = stageInfo.getStageStats();
//
//        ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
//        for (StageInfo subStage : stageInfo.getSubStages()) {
//            subStages.add(toStageStats(subStage));
//        }
//
//        Set<String> uniqueNodes = new HashSet<>();
//        for (TaskInfo task : stageInfo.getTasks()) {
//            // todo add nodeId to TaskInfo
//            URI uri = task.getSelf();
//            uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
//        }
//
//        return StageStats.builder().setStageId(String.valueOf(stageInfo.getStageId().getId()))
//                .setState(stageInfo.getState().toString()).setDone(stageInfo.getState().isDone())
//                .setNodes(uniqueNodes.size()).setTotalSplits(stageStats.getTotalDrivers())
//                .setQueuedSplits(stageStats.getQueuedDrivers()).setRunningSplits(stageStats.getRunningDrivers())
//                .setCompletedSplits(stageStats.getCompletedDrivers())
//                .setUserTimeMillis(stageStats.getTotalUserTime().toMillis())
//                .setCpuTimeMillis(stageStats.getTotalCpuTime().toMillis())
//                .setWallTimeMillis(stageStats.getTotalScheduledTime().toMillis())
//                .setProcessedRows(stageStats.getRawInputPositions())
//                .setProcessedBytes(stageStats.getRawInputDataSize().toBytes()).setSubStages(subStages.build()).build();
//    }
//
//    private static Set<String> globalUniqueNodes(StageInfo stageInfo) {
//        if (stageInfo == null) {
//            return ImmutableSet.of();
//        }
//        ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
//        for (TaskInfo task : stageInfo.getTasks()) {
//            // todo add nodeId to TaskInfo
//            URI uri = task.getSelf();
//            nodes.add(uri.getHost() + ":" + uri.getPort());
//        }
//
//        for (StageInfo subStage : stageInfo.getSubStages()) {
//            nodes.addAll(globalUniqueNodes(subStage));
//        }
//        return nodes.build();
//    }
//
//    private static boolean isScheduled(QueryInfo queryInfo) {
//        StageInfo stage = queryInfo.getOutputStage();
//        if (stage == null) {
//            return false;
//        }
//        return IterableTransformer.on(getAllStages(stage)).transform(stageStateGetter()).all(isStageRunningOrDone());
//    }
//
//    private static Predicate<StageState> isStageRunningOrDone() {
//        return new Predicate<StageState>() {
//            @Override
//            public boolean apply(StageState state) {
//                return (state == StageState.RUNNING) || state.isDone();
//            }
//        };
//    }
//
//    private static URI findCancelableLeafStage(QueryInfo queryInfo) {
//        if (queryInfo.getOutputStage() == null) {
//            // query is not running yet, cannot cancel leaf stage
//            return null;
//        }
//
//        // query is running, find the leaf-most running stage
//        return findCancelableLeafStage(queryInfo.getOutputStage());
//    }
//
//    private static URI findCancelableLeafStage(StageInfo stage) {
//        // if this stage is already done, we can't cancel it
//        if (stage.getState().isDone()) {
//            return null;
//        }
//
//        // attempt to find a cancelable sub stage
//        // check in reverse order since build side of a join will be later in the list
//        for (StageInfo subStage : Lists.reverse(stage.getSubStages())) {
//            URI leafStage = findCancelableLeafStage(subStage);
//            if (leafStage != null) {
//                return leafStage;
//            }
//        }
//
//        // no matching sub stage, so return this stage
//        return stage.getSelf();
//    }
//
//    private static QueryError toQueryError(QueryInfo queryInfo) {
//        FailureInfo failure = queryInfo.getFailureInfo();
//        if (failure == null) {
//            QueryState state = queryInfo.getState();
//            if ((!state.isDone()) || (state == QueryState.FINISHED)) {
//                return null;
//            }
//            log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
//            failure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state)));
//        }
//        return new QueryError(failure.getMessage(), null, 0, failure.getErrorLocation(), failure);
//    }
//
//    private static class RowIterable implements Iterable<List<Object>> {
//        private final Page page;
//
//        private RowIterable(Page page) {
//            this.page = checkNotNull(page, "page is null");
//        }
//
//        @Override
//        public Iterator<List<Object>> iterator() {
//            return new RowIterator(page);
//        }
//    }
//
//    private static class RowIterator extends AbstractIterator<List<Object>> {
//        private final BlockCursor[] cursors;
//        private final int columnCount;
//
//        private RowIterator(Page page) {
//            int columnCount = 0;
//            cursors = new BlockCursor[page.getChannelCount()];
//            for (int channel = 0; channel < cursors.length; channel++) {
//                cursors[channel] = page.getBlock(channel).cursor();
//                columnCount = cursors[channel].getTupleInfo().getFieldCount();
//            }
//            this.columnCount = columnCount;
//        }
//
//        @Override
//        protected List<Object> computeNext() {
//            List<Object> row = new ArrayList<>(columnCount);
//            for (BlockCursor cursor : cursors) {
//                if (!cursor.advanceNextPosition()) {
//                    Preconditions.checkState(row.isEmpty(), "Page is unaligned");
//                    return endOfData();
//                }
//
//                row.addAll(cursor.getTuple().toValues());
//            }
//            return row;
//        }
//    }
// }
