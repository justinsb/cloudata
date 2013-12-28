package com.cloudata.structured.sql;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.structured.StructuredStore;
import com.cloudata.structured.sql.provider.CloudataConnector;
import com.cloudata.structured.sql.provider.CloudataConnectorFactory;
import com.cloudata.structured.sql.simple.SimpleExecutor;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.OutputTableHandleResolver;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorOutputHandleResolver;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.storage.StorageManager;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SqlEngine {
    private static final Logger log = LoggerFactory.getLogger(SqlEngine.class);

    final List<PlanOptimizer> planOptimizers;
    final PeriodicImportManager periodicImportManager;
    final StorageManager storageManager;
    final MetadataManager metadataManager;

    final ConnectorManager connectorManager;

    private final DataStreamManager dataStreamManager;

    final ExecutorService executor;

    final CloudataConnector cloudataConnector;

    private final StructuredStore store;

    String catalogName = "default";

    public SqlEngine(StructuredStore store, ExecutorService executor) {
        this.store = store;
        this.executor = executor;
        MetadataManager metadataManager = new MetadataManager();

        SplitManager splitManager = new SplitManager(Sets.<ConnectorSplitManager> newHashSet());

        this.dataStreamManager = new DataStreamManager();
        HandleResolver handleResolver = new HandleResolver();
        Map<String, ConnectorFactory> connectorFactories = Maps.newHashMap();
        Map<String, Connector> globalConnectors = Maps.newHashMap();

        RecordSinkManager recordSinkManager = new RecordSinkManager();
        Map<String, ConnectorOutputHandleResolver> handleIdResolvers = Maps.newHashMap();
        OutputTableHandleResolver outputTableHandleResolver = new OutputTableHandleResolver(handleIdResolvers);

        this.connectorManager = new ConnectorManager(metadataManager, splitManager, dataStreamManager,
                recordSinkManager, handleResolver, outputTableHandleResolver, connectorFactories, globalConnectors);

        // NodeManager nodeManager = new InMemoryNodeManager();
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadataManager, splitManager);
        List<PlanOptimizer> planOptimizers = planOptimizersFactory.get();

        this.metadataManager = metadataManager;
        this.planOptimizers = planOptimizers;
        this.periodicImportManager = new StubPeriodicImportManager();
        this.storageManager = new StubStorageManager();

        NodeManager nodeManager = new InMemoryNodeManager();
        CloudataConnectorFactory cloudataConnectorFactory = new CloudataConnectorFactory(nodeManager,
                Maps.<String, String> newHashMap(), store);

        connectorManager.addConnectorFactory(cloudataConnectorFactory);

        connectorManager.createConnection(catalogName, CloudataConnectorFactory.PROVIDER_ID,
                Maps.<String, String> newHashMap());

        this.cloudataConnector = cloudataConnectorFactory.get(catalogName);
    }

    // public void addGlobalConnector(String connectorId, Connector connector) {
    // connectorManager.addGlobalConnector(connectorId, connector);
    // }

    public SqlStatement parse(SqlSession session, String sql) {
        log.debug("Parsing sql: {}", sql);

        Statement statement = SqlParser.createStatement(sql);

        QueryExplainer queryExplainer = new QueryExplainer(session.prestoSession, planOptimizers, metadataManager,
                periodicImportManager, storageManager);
        // analyze query
        Analyzer analyzer = new Analyzer(session.prestoSession, metadataManager, Optional.of(queryExplainer));

        Analysis analysis = analyzer.analyze(statement);

        // System.out.println("analysis: " + analysis);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(session.prestoSession, planOptimizers, idAllocator,
                metadataManager, periodicImportManager, storageManager);
        Plan plan = logicalPlanner.plan(analysis);

        return new SqlStatement(session, sql, plan);
        //
        // TableScanCountVisitor visitor = new TableScanCountVisitor();
        // plan.getRoot().accept(visitor, 0);
        // Assert.assertEquals(1, visitor.count);
        // String p = PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes());
        //
        // System.out.println("plan: " + p);
    }

    public SqlSession createSession() {
        return new SqlSession(this, catalogName);
    }

    void execute(SqlSession sqlSession, SqlStatement sqlStatement, final RowsetListener listener) {
        SimpleExecutor executor = new SimpleExecutor(this, sqlSession, sqlStatement);
        executor.execute(listener);

    }

    public Metadata getMetadata() {
        return this.metadataManager;
    }

    public ExecutorService getExecutor() {
        return this.executor;
    }

    public RecordSet getRecordSet(TableMetadata tableMetadata) {
        return cloudataConnector.getRecordset(tableMetadata);
    }
}
