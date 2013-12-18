package com.cloudata.structured.sql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

import com.cloudata.structured.sql.provider.CloudataConnectorMetadata;
import com.cloudata.structured.sql.provider.CloudataSplitManager;
import com.cloudata.structured.sql.simple.SimpleNode;
import com.cloudata.structured.sql.simple.SimpleTreePrinter;
import com.facebook.presto.client.Column;
import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplitManager;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.NodeScheduler;
import com.facebook.presto.execution.NodeSchedulerConfig;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlQueryExecution;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.storage.StorageManager;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

public class SqlTest {
    String connectorId = "connector0";
    String catalog = "default";

    // @Test
    public void execute() throws InterruptedException {
        MetadataManager metadata = new MetadataManager();
        {
            DualMetadata dualMetadata = new DualMetadata();
            metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, dualMetadata);
        }

        // TaskExecutor taskExecutor = new TaskExecutor(8);
        // taskExecutor.start();

        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        SplitManager splitManager = new SplitManager(Sets.<ConnectorSplitManager> newHashSet());
        {
            DualSplitManager dualSplitManager = new DualSplitManager(nodeManager);
            splitManager.addConnectorSplitManager(dualSplitManager);
        }

        QueryId queryId = new QueryId(UUID.randomUUID().toString().replace("-", ""));
        // String query = "SELECT * FROM table1 JOIN table2 ON table1.key1=table2.key1 WHERE table1.key2='1'";
        String query = "SELECT " + DualMetadata.COLUMN_NAME + " FROM " + DualMetadata.NAME;

        // URI self = URI.create("localhost");

        Statement statement = SqlParser.createStatement(query);

        // URI baseUri = URI.create("http://localhost:1234");
        // Node me = new Node("me", baseUri, NodeVersion.UNKNOWN);

        // NodeManager nodeManager = new InMemoryNodeManager(me);
        NodeSchedulerConfig config = new NodeSchedulerConfig();
        NodeScheduler nodeScheduler = new NodeScheduler(nodeManager, config);
        //
        List<PlanOptimizer> planOptimizers = buildPlanOptimizers(metadata, splitManager);
        //

        ExecutorService queryExecutor = Executors.newCachedThreadPool(); // threadsNamed("query-scheduler-%d"));

        RemoteTaskFactory remoteTaskFactory = new FakeRemoteTaskFactory(queryExecutor);
        LocationFactory locationFactory = new MockLocationFactory(); // nodeManager, baseUri);
        int maxPendingSplitsPerNode = 1;
        int initialHashPartitions = 1;
        ShardManager shardManager = new MockShardManager();
        StorageManager storageManager = new MockStorageManager();
        PeriodicImportManager periodicImportManager = new MockPeriodicImportManager();

        URI selfUri = nodeManager.getCurrentNode().getHttpUri();
        SqlQueryExecution queryExecution = new SqlQueryExecution(queryId, query, buildSession(), selfUri, statement,
                metadata, splitManager, nodeScheduler, planOptimizers, remoteTaskFactory, locationFactory,
                maxPendingSplitsPerNode, initialHashPartitions, queryExecutor, shardManager, storageManager,
                periodicImportManager);

        queryExecution.addStateChangeListener(new StateChangeListener<QueryState>() {

            @Override
            public void stateChanged(QueryState newValue) {
                System.out.println("New state: " + newValue);

            }
        });

        queryExecution.start();

        StageInfo outputStage = queryExecution.getQueryInfo().getOutputStage();
        Thread.sleep(30000);

        System.out.println(queryExecution.getQueryInfo().getFailureInfo());

        // List<TaskSource> sources = ImmutableList.<TaskSource> of(new TaskSource(tableScanNodeId, ImmutableSet
        // .of(new ScheduledSplit(0, split)), true));

        // taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        // assertEquals(taskInfo.getState(), TaskState.RUNNING);
        //
        // BufferResult results = sqlTaskManager.getTaskResults(taskId, "out", 0, new DataSize(1, Unit.MEGABYTE),
        // new Duration(1, TimeUnit.SECONDS));
        // assertEquals(results.isBufferClosed(), false);
        // assertEquals(results.getPages().size(), 1);
        // assertEquals(results.getPages().get(0).getPositionCount(), 1);
        //
        // results = sqlTaskManager.getTaskResults(taskId, "out", results.getToken() + results.getPages().size(),
        // new DataSize(1, Unit.MEGABYTE), new Duration(1, TimeUnit.SECONDS));
        // // todo this should be true
        // assertEquals(results.isBufferClosed(), false);
        // assertEquals(results.getPages().size(), 0);
        //
        // sqlTaskManager.waitForStateChange(taskInfo.getTaskId(), taskInfo.getState(), new Duration(1,
        // TimeUnit.SECONDS));
        // taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        // assertEquals(taskInfo.getState(), TaskState.FINISHED);
        // taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        // assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    private static List<Column> createColumnsList(QueryInfo queryInfo) {
        checkNotNull(queryInfo, "queryInfo is null");
        StageInfo outputStage = queryInfo.getOutputStage();
        if (outputStage == null) {
            checkNotNull(outputStage, "outputStage is null");
        }

        List<String> names = queryInfo.getFieldNames();
        ArrayList<Type> types = new ArrayList<>();
        for (TupleInfo tupleInfo : outputStage.getTupleInfos()) {
            types.addAll(tupleInfo.getTypes());
        }

        checkArgument(names.size() == types.size(), "names and types size mismatch");

        ImmutableList.Builder<Column> list = ImmutableList.builder();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Type type = types.get(i);
            switch (type) {
            case BOOLEAN:
                list.add(new Column(name, "boolean"));
                break;
            case FIXED_INT_64:
                list.add(new Column(name, "bigint"));
                break;
            case DOUBLE:
                list.add(new Column(name, "double"));
                break;
            case VARIABLE_BINARY:
                list.add(new Column(name, "varchar"));
                break;
            default:
                throw new IllegalArgumentException("unhandled type: " + type);
            }
        }
        return list.build();
    }

    // @Test
    // public void runQuery() throws IOException, InterruptedException {
    // DualMetadata dualMetadata = new DualMetadata();
    // // TableHandle tableHandle = dualMetadata.getTableHandle(new SchemaTableName("default", DualMetadata.NAME));
    // // assertNotNull(tableHandle);
    // //
    // // ColumnHandle columnHandle = dualMetadata.getColumnHandle(tableHandle, DualMetadata.COLUMN_NAME);
    // // assertNotNull(columnHandle);
    // // Symbol symbol = new Symbol(DualMetadata.COLUMN_NAME);
    //
    // MetadataManager metadata = new MetadataManager();
    // metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, dualMetadata);
    //
    // TaskExecutor taskExecutor = new TaskExecutor(8);
    // taskExecutor.start();
    //
    // InMemoryNodeManager nodeManager = new InMemoryNodeManager();
    //
    // SplitManager splitManager = new SplitManager(Sets.<ConnectorSplitManager> newHashSet());
    // {
    // DualSplitManager dualSplitManager = new DualSplitManager(nodeManager);
    // splitManager.addConnectorSplitManager(dualSplitManager);
    // }
    //
    // StorageManager storageManager = new MockStorageManager();
    // PeriodicImportManager periodicImportManager = new MockPeriodicImportManager();
    //
    // List<PlanOptimizer> planOptimizers = buildPlanOptimizers(metadata, splitManager);
    //
    // PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    //
    // for (int i = 0; i < 5; i++) {
    // String sql = "SELECT " + DualMetadata.COLUMN_NAME + " FROM " + DualMetadata.NAME;
    //
    // Statement statement = SqlParser.createStatement(sql);
    //
    // System.out.println("Statement: " + statement);
    //
    // Session session = buildSession();
    // QueryExplainer queryExplainer = new QueryExplainer(session, planOptimizers, metadata,
    // periodicImportManager, storageManager);
    // // analyze query
    // Analyzer analyzer = new Analyzer(session, metadata, Optional.of(queryExplainer));
    //
    // Analysis analysis = analyzer.analyze(statement);
    //
    // System.out.println("analysis: " + analysis);
    //
    // // plan query
    // LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata,
    // periodicImportManager, storageManager);
    // Plan plan = logicalPlanner.plan(analysis);
    //
    // String p = PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes());
    // System.out.println("plan: " + p);
    //
    // List<Input> inputs = new InputExtractor(metadata).extract(plan.getRoot());
    // // stateMachine.setInputs(inputs);
    //
    // // fragment the plan
    // SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubPlans(plan, false);
    //
    // PlanFragment planFragment = subplan.getFragment();
    // // PartitionResult partitionResult = splitManager.getPartitions(tableHandle,
    // // Optional.of(TupleDomain.all()));
    // // Split split = Iterables.getOnlyElement(splitManager.getPartitionSplits(tableHandle,
    // // partitionResult.getPartitions()).getSplits());
    //
    // LocalExecutionPlanner planner = new LocalExecutionPlanner(new NodeInfo("test"), metadata,
    // new DataStreamManager(new DualDataStreamProvider()), new MockLocalStorageManager(),
    // new MockExchangeClientSupplier(), new ExpressionCompiler(metadata));
    //
    // SqlTaskManager sqlTaskManager = new SqlTaskManager(planner, new MockLocationFactory(), taskExecutor,
    // new QueryMonitor(new ObjectMapperProvider().get(), new NullEventClient(), new NodeInfo("test")),
    // new TaskManagerConfig());
    //
    // // PlanNodeId tableScanNodeId = new PlanNodeId("tableScan");
    // // PlanFragment testFragment = new PlanFragment(new PlanFragmentId("fragment"), new TableScanNode(
    // // tableScanNodeId, tableHandle, ImmutableList.of(symbol), ImmutableMap.of(symbol, columnHandle),
    // // Optional.<GeneratedPartitions> absent()), ImmutableMap.<Symbol, Type> of(symbol, Type.VARCHAR),
    // // Partitioning.SOURCE, tableScanNodeId);
    //
    // TaskId taskId = new TaskId("query", "stage", "task");
    //
    // List<TaskSource> sources;
    // {
    // ImmutableList.Builder<TaskSource> sb = ImmutableList.builder();
    // for (PlanNodeId planNodeId : planFragment.getSourceIds()) {
    // ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
    // if (sourceId != null) {
    // initialSplits.put(sourceId, sourceSplit);
    // }
    // for (Entry<PlanNodeId, URI> entry : exchangeLocations.get().entries()) {
    // initialSplits.put(entry.getKey(),
    // createRemoteSplitFor(node.getNodeIdentifier(), entry.getValue()));
    // }
    //
    // Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
    // boolean noMoreSplits = this.noMoreSplits.contains(planNodeId);
    // if (!splits.isEmpty() || noMoreSplits) {
    // sb.add(new TaskSource(planNodeId, splits, noMoreSplits));
    // }
    // }
    // sources = sb.build();
    // }
    // // List<TaskSource> sources = ImmutableList.<TaskSource> of(new TaskSource(tableScanNodeId, ImmutableSet
    // // .of(new ScheduledSplit(0, split)), true));
    // OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer("out",
    // new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds();
    // TaskInfo taskInfo = sqlTaskManager.updateTask(session, taskId, testFragment, sources, outputBuffers);
    // assertEquals(taskInfo.getState(), TaskState.RUNNING);
    //
    // taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
    // assertEquals(taskInfo.getState(), TaskState.RUNNING);
    //
    // BufferResult results = sqlTaskManager.getTaskResults(taskId, "out", 0, new DataSize(1, Unit.MEGABYTE),
    // new Duration(1, TimeUnit.SECONDS));
    // assertEquals(results.isBufferClosed(), false);
    // assertEquals(results.getPages().size(), 1);
    // assertEquals(results.getPages().get(0).getPositionCount(), 1);
    //
    // results = sqlTaskManager.getTaskResults(taskId, "out", results.getToken() + results.getPages().size(),
    // new DataSize(1, Unit.MEGABYTE), new Duration(1, TimeUnit.SECONDS));
    // // todo this should be true
    // assertEquals(results.isBufferClosed(), false);
    // assertEquals(results.getPages().size(), 0);
    //
    // sqlTaskManager.waitForStateChange(taskInfo.getTaskId(), taskInfo.getState(), new Duration(1,
    // TimeUnit.SECONDS));
    // taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
    // assertEquals(taskInfo.getState(), TaskState.FINISHED);
    // taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
    // assertEquals(taskInfo.getState(), TaskState.FINISHED);
    // }
    // }

    private Session buildSession() {
        String user = "user";
        String source = "test";
        String schema = "default";
        String remoteUserAddress = "remoteUserAddress";
        String userAgent = "userAgent";
        Session session = new Session(user, source, catalog, schema, remoteUserAddress, userAgent);
        return session;
    }

    @Test
    public void testParse() {
        // String sql = "SELECT * FROM table1 JOIN table2 ON table1.key1=table2.key1 WHERE table1.key2='1'";
        // String sql = "SELECT * FROM table1  WHERE table1.key2>'1'";

        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        MetadataManager metadata = buildMetadata();
        StorageManager storageManager = new MockStorageManager();
        PeriodicImportManager periodicImportManager = new MockPeriodicImportManager();

        SplitManager splitManager = buildSplitManager(nodeManager);
        List<PlanOptimizer> planOptimizers = buildPlanOptimizers(metadata, splitManager);

        for (int i = 0; i < 1; i++) {
            // String sql = "SELECT key1 as k1, key2 || 'hello' as k2, 'world' as k3 FROM table1";
            String sql = "SELECT * FROM table1 JOIN table1 t2 ON table1.key1=t2.key1";

            Statement statement = SqlParser.createStatement(sql);

            // System.out.println("Statement: " + statement);

            Session session = buildSession();
            QueryExplainer queryExplainer = new QueryExplainer(session, planOptimizers, metadata,
                    periodicImportManager, storageManager);
            // analyze query
            Analyzer analyzer = new Analyzer(session, metadata, Optional.of(queryExplainer));

            Analysis analysis = analyzer.analyze(statement);

            // System.out.println("analysis: " + analysis);

            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            // plan query
            LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata,
                    periodicImportManager, storageManager);
            Plan plan = logicalPlanner.plan(analysis);

            TableScanCountVisitor visitor = new TableScanCountVisitor();
            plan.getRoot().accept(visitor, 0);
            // Assert.assertEquals(1, visitor.count);
            String p = PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes());

            System.out.println("plan: " + p);
        }

        // plan: - Output[key1, key2]
        // - TableScan[com.cloudata.structured.sql.MockTableHandle@6c6dba0d, domain={}] => [key1:varchar, key2:varchar]
        // key1 := com.cloudata.structured.sql.MockColumnHandle@319560e6
        // key2 := com.cloudata.structured.sql.MockColumnHandle@460cb578

        // plan: - Output[key1, key2]
        // - Filter[("key2" > '1')] => [key1:varchar, key2:varchar]
        // - TableScan[com.cloudata.structured.sql.MockTableHandle@6d3a3c8e, domain={}] => [key1:varchar, key2:varchar]
        // key1 := com.cloudata.structured.sql.MockColumnHandle@5bb10cf0
        // key2 := com.cloudata.structured.sql.MockColumnHandle@6311c509
        //

        // List<Input> inputs = new InputExtractor(metadata).extract(plan.getRoot());
        // stateMachine.setInputs(inputs);

        // // fragment the plan
        // SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubPlans(plan, false);
        //
        // stateMachine.recordAnalysisTime(analysisStart);
        // return subplan;
        // }

    }

    @Test
    public void testEngine() {
        MetadataManager metadata = buildMetadata();
        StorageManager storageManager = new MockStorageManager();
        PeriodicImportManager periodicImportManager = new MockPeriodicImportManager();

        NodeManager nodeManager = new InMemoryNodeManager();

        SplitManager splitManager = buildSplitManager(nodeManager);
        List<PlanOptimizer> planOptimizers = buildPlanOptimizers(metadata, splitManager);

        SqlSession session = new SqlSession();
        SqlEngine engine = new SqlEngine(metadata, planOptimizers, periodicImportManager, storageManager);

        String sql = "SELECT key1 as k1 FROM table1";

        SqlStatement statement = engine.parse(session, sql);
        Assert.assertTrue(statement.isSimple());

        SimpleNode simple = statement.getSimple();
        Assert.assertNotNull(simple);

        System.out.println(SimpleTreePrinter.toString(simple));
    }

    private List<PlanOptimizer> buildPlanOptimizers(MetadataManager metadata, SplitManager splitManager) {
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadata, splitManager);
        List<PlanOptimizer> planOptimizers = planOptimizersFactory.get();
        return planOptimizers;
    }

    private SplitManager buildSplitManager(NodeManager nodeManager) {
        SplitManager splitManager = new SplitManager(Sets.<ConnectorSplitManager> newHashSet());
        splitManager.addConnectorSplitManager(new CloudataSplitManager(nodeManager, connectorId));
        return splitManager;
    }

    private MetadataManager buildMetadata() {
        // Metadata metadata = new TestMetadata();
        MetadataManager metadata = new MetadataManager();

        metadata.addConnectorMetadata(connectorId, catalog, new CloudataConnectorMetadata(connectorId));

        return metadata;
    }
}
