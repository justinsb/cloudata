package com.cloudata.structured.sql;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.storage.StorageManager;
import com.google.common.base.Optional;

public class SqlEngine {
    private static final Logger log = LoggerFactory.getLogger(SqlEngine.class);

    final MetadataManager metadata;
    final List<PlanOptimizer> planOptimizers;
    final PeriodicImportManager periodicImportManager;
    final StorageManager storageManager;

    // SplitManager splitManager = buildSplitManager();

    public SqlEngine(MetadataManager metadata, List<PlanOptimizer> planOptimizers,
            PeriodicImportManager periodicImportManager, StorageManager storageManager) {
        this.metadata = metadata;
        this.planOptimizers = planOptimizers;
        this.periodicImportManager = periodicImportManager;
        this.storageManager = storageManager;
    }

    public SqlStatement parse(SqlSession session, String sql) {
        log.debug("Parsing sql: {}", sql);

        Statement statement = SqlParser.createStatement(sql);

        QueryExplainer queryExplainer = new QueryExplainer(session.prestoSession, planOptimizers, metadata,
                periodicImportManager, storageManager);
        // analyze query
        Analyzer analyzer = new Analyzer(session.prestoSession, metadata, Optional.of(queryExplainer));

        Analysis analysis = analyzer.analyze(statement);

        // System.out.println("analysis: " + analysis);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(session.prestoSession, planOptimizers, idAllocator,
                metadata, periodicImportManager, storageManager);
        Plan plan = logicalPlanner.plan(analysis);

        return new SqlStatement(metadata, sql, plan);
        //
        // TableScanCountVisitor visitor = new TableScanCountVisitor();
        // plan.getRoot().accept(visitor, 0);
        // Assert.assertEquals(1, visitor.count);
        // String p = PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes());
        //
        // System.out.println("plan: " + p);
    }

}
