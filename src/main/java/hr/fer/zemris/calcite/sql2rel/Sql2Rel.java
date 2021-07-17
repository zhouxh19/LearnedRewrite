package hr.fer.zemris.calcite.sql2rel;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.SourceStringReader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;


public class Sql2Rel {
    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        final DataSource ds = JdbcSchema.dataSource( // "jdbc:postgresql://166.111.121.62:5432/tpch1x",
                "jdbc:postgresql://localhost:5432/randy",
                "org.postgresql.Driver",
                "randy",
                "randy"
        );
        SchemaPlus schema = rootSchema.add("randy", JdbcSchema.create(rootSchema, "randy", ds, null, null));
        //System.out.println(rootSchema.toString());
        System.out.println("============");
        System.out.println(rootSchema.getSubSchema("randy").getTableNames());
        System.out.println("============");

        FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(schema).build();
        Planner planner = Frameworks.getPlanner(config);

        // sql parsing (sqlNode)
        SqlNode sqlNode = planner.parse(new SourceStringReader(args[0]));       // planner.parse
        //System.out.println(sqlNode.toString());
        sqlNode = planner.validate(sqlNode);                                    // planner.validate

        //System.out.println(" ");
        //System.out.println(sqlNode.toString());
        //System.out.println(" ");

        // sql rewrite
        RelRoot relRoot = planner.rel(sqlNode);                                 // planner.SQLtoRA
        RelNode relNode = relRoot.project();                                    // fetch the whole RA expression
        // relRoot = relRoot.rel;
        // System.out.println(RelOptUtil.toString(relNode));

        // HashMap<String, String> ruleMap = new HashMap<>();
        // ruleMap.put("0","AggregateExpandDistinctAggregatesRule.INSTANCE");

        // Left rules
        //  CalcRelSplitter.RelType
        //  CoerceInputsRule
        //  EquiJoin
        //          LoptJoinTree
        //  LoptMultiJoin
        //          LoptSemiJoinOptimizer
        //  MultiJoin
        //          PushProjector
        //  rules.add(AggregateExtractProjectRule.);
        //  rules.add(FilterJoinRule.TRUE_PREDICATE);

        List<RelOptRule> rules = new ArrayList<RelOptRule>();
        if (args[1].equals("default")) {
            //rules.add(FilterMergeRule.INSTANCE);
            System.out.println(" default model");
        } else { // 126 rules
//            rules.add(UnionEliminatorRule.INSTANCE);
//            rules.add(UnionMergeRule.INSTANCE);
//            rules.add(UnionMergeRule.INTERSECT_INSTANCE);
//            rules.add(UnionMergeRule.MINUS_INSTANCE);
//            rules.add(UnionPullUpConstantsRule.INSTANCE);
//            rules.add(UnionToDistinctRule.INSTANCE);
//            rules.add(FilterJoinRule.FILTER_ON_JOIN);
//            rules.add(FilterJoinRule.DUMB_FILTER_ON_JOIN);
//            rules.add(TableScanRule.INSTANCE);
//            // rules.add(AggregateExpandDistinctAggregatesRule.INSTANCE);
//            // rules.add(AggregateFilterTransposeRule.INSTANCE);
//            // //rules.add(AggregateJoinTransposeRule.INSTANCE);
//            // rules.add(AggregateProjectMergeRule.INSTANCE);
//            // rules.add(AggregateProjectPullUpConstantsRule.INSTANCE);
//            // rules.add(AggregateReduceFunctionsRule.INSTANCE);
//            // rules.add(AggregateRemoveRule.INSTANCE);
//            // rules.add(AggregateStarTableRule.INSTANCE);
//            // rules.add(AggregateUnionAggregateRule.INSTANCE);
//            // rules.add(AggregateUnionTransposeRule.INSTANCE);
//            // rules.add(AggregateValuesRule.INSTANCE);
//            // rules.add(CalcMergeRule.INSTANCE);
//            // rules.add(CalcRemoveRule.INSTANCE);
//            // rules.add(CalcSplitRule.INSTANCE);
//            // rules.add(DateRangeRules.FILTER_INSTANCE);
//            // rules.add(FilterAggregateTransposeRule.INSTANCE);
//            // rules.add(FilterCalcMergeRule.INSTANCE);
//            // rules.add(FilterCorrelateRule.INSTANCE);
//            // rules.add(FilterMergeRule.INSTANCE);
//            // rules.add(FilterMultiJoinMergeRule.INSTANCE);
//            // rules.add(FilterProjectTransposeRule.INSTANCE);
//            // rules.add(FilterRemoveIsNotDistinctFromRule.INSTANCE);
//            // rules.add(FilterSetOpTransposeRule.INSTANCE);
//            // rules.add(FilterTableFunctionTransposeRule.INSTANCE);
//            // rules.add(FilterTableScanRule.INSTANCE);
//            // rules.add(FilterTableScanRule.INTERPRETER);
//            // rules.add(FilterToCalcRule.INSTANCE);
//            // rules.add(IntersectToDistinctRule.INSTANCE);
//            // rules.add(JoinAddRedundantSemiJoinRule.INSTANCE);
//            // rules.add(JoinAssociateRule.INSTANCE);
//            // rules.add(JoinCommuteRule.INSTANCE);
//            // rules.add(JoinCommuteRule.SWAP_OUTER);
//            // rules.add(JoinExtractFilterRule.INSTANCE);
//            // rules.add(JoinProjectTransposeRule.BOTH_PROJECT);
//            // rules.add(JoinProjectTransposeRule.BOTH_PROJECT_INCLUDE_OUTER);
//            // rules.add(JoinProjectTransposeRule.LEFT_PROJECT);
//            // rules.add(JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER);
//            // rules.add(JoinProjectTransposeRule.RIGHT_PROJECT);
//            // rules.add(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER);
//            // rules.add(JoinPushExpressionsRule.INSTANCE);
//            // rules.add(JoinPushThroughJoinRule.LEFT);
//            // rules.add(JoinPushThroughJoinRule.RIGHT);
//            // rules.add(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER);
//            // rules.add(JoinProjectTransposeRule.RIGHT_PROJECT);
//            // rules.add(JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER);
//            // rules.add(JoinProjectTransposeRule.LEFT_PROJECT);
//            // rules.add(JoinProjectTransposeRule.RIGHT_PROJECT);
//            // rules.add(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER);
//            // rules.add(JoinProjectTransposeRule.BOTH_PROJECT_INCLUDE_OUTER);
//            // rules.add(JoinProjectTransposeRule.BOTH_PROJECT);
//            // rules.add(JoinPushTransitivePredicatesRule.INSTANCE);
//            // rules.add(JoinToCorrelateRule.JOIN);
//            // rules.add(JoinToCorrelateRule.SEMI);
//            // rules.add(JoinToMultiJoinRule.INSTANCE);
//            // rules.add(JoinUnionTransposeRule.LEFT_UNION);
//            // rules.add(JoinUnionTransposeRule.RIGHT_UNION);
//            rules.add(LoptOptimizeJoinRule.INSTANCE);
//            rules.add(MaterializedViewFilterScanRule.INSTANCE);
//            // rules.add(MultiJoinOptimizeBushyRule.INSTANCE);
//            // rules.add(MultiJoinProjectTransposeRule.BOTH_PROJECT);
//            // rules.add(MultiJoinProjectTransposeRule.BOTH_PROJECT_INCLUDE_OUTER);
//            // rules.add(MultiJoinProjectTransposeRule.LEFT_PROJECT);
//            // rules.add(MultiJoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER);
//            // rules.add(MultiJoinProjectTransposeRule.MULTI_BOTH_PROJECT);
//            // rules.add(MultiJoinProjectTransposeRule.MULTI_LEFT_PROJECT);
//            // rules.add(MultiJoinProjectTransposeRule.MULTI_RIGHT_PROJECT);
//            // rules.add(MultiJoinProjectTransposeRule.RIGHT_PROJECT);
//            // rules.add(MultiJoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER);
//            rules.add(ProjectCalcMergeRule.INSTANCE);
//            rules.add(ProjectCorrelateTransposeRule.INSTANCE);
//            rules.add(ProjectFilterTransposeRule.INSTANCE);
//            // rules.add(ProjectJoinTransposeRule.INSTANCE);
//            rules.add(ProjectMergeRule.INSTANCE);
//            // rules.add(ProjectMultiJoinMergeRule.INSTANCE);
//            rules.add(ProjectRemoveRule.INSTANCE);
//            rules.add(ProjectSetOpTransposeRule.INSTANCE);
//            rules.add(ProjectSortTransposeRule.INSTANCE);
//            rules.add(ProjectTableScanRule.INSTANCE);
//            // rules.add(ProjectToCalcRule.INSTANCE);                           // take care!
//            rules.add(ProjectToWindowRule.INSTANCE);
//            rules.add(ProjectToWindowRule.PROJECT);
//            rules.add(ProjectWindowTransposeRule.INSTANCE);
//            rules.add(PruneEmptyRules.AGGREGATE_INSTANCE);
//            rules.add(PruneEmptyRules.FILTER_INSTANCE);
//            rules.add(PruneEmptyRules.INTERSECT_INSTANCE);
//            // rules.add(PruneEmptyRules.JOIN_LEFT_INSTANCE);
//            // rules.add(PruneEmptyRules.JOIN_RIGHT_INSTANCE);
//            rules.add(PruneEmptyRules.MINUS_INSTANCE);
//            rules.add(PruneEmptyRules.PROJECT_INSTANCE);
//            rules.add(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);
//            rules.add(PruneEmptyRules.SORT_INSTANCE);
//            rules.add(PruneEmptyRules.UNION_INSTANCE);
//            rules.add(ReduceDecimalsRule.INSTANCE);
//            rules.add(ReduceExpressionsRule.CALC_INSTANCE);
//            rules.add(ReduceExpressionsRule.FILTER_INSTANCE);
//            // rules.add(ReduceExpressionsRule.JOIN_INSTANCE);
//            rules.add(ReduceExpressionsRule.PROJECT_INSTANCE);
//            // rules.add(SemiJoinFilterTransposeRule.INSTANCE);
//            // rules.add(SemiJoinJoinTransposeRule.INSTANCE);
//            // rules.add(SemiJoinProjectTransposeRule.INSTANCE);
//            // rules.add(SemiJoinRemoveRule.INSTANCE);
//            // rules.add(SemiJoinRule.JOIN);
//            // rules.add(SemiJoinRule.PROJECT);
//            // rules.add(SortJoinTransposeRule.INSTANCE);
//            rules.add(SortProjectTransposeRule.INSTANCE);
//            rules.add(SortRemoveConstantKeysRule.INSTANCE);
//            rules.add(SortRemoveRule.INSTANCE);
//            rules.add(SortUnionTransposeRule.INSTANCE);
//            rules.add(SortUnionTransposeRule.MATCH_NULL_FETCH);
//            rules.add(SubQueryRemoveRule.FILTER);
//            // rules.add(SubQueryRemoveRule.JOIN);
//            rules.add(SubQueryRemoveRule.PROJECT);
//            rules.add(ValuesReduceRule.FILTER_INSTANCE);
//            rules.add(ValuesReduceRule.PROJECT_FILTER_INSTANCE);
//            rules.add(ValuesReduceRule.PROJECT_INSTANCE);
            // Rule description 'MultiJoinProjectTransposeRule: with two LogicalProject children' is not valid
        }

        HepProgram program = new HepProgramBuilder().addMatchOrder(HepMatchOrder.TOP_DOWN).addRuleCollection(rules).build();

        HepPlanner hepPlanner = new HepPlanner(program);
        hepPlanner.setRoot(relNode);
        relNode = hepPlanner.findBestExp();                                     // RelNode

        System.out.println(RelOptUtil.toString(relNode));
    }
}



