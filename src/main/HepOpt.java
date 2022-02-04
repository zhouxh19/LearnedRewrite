package main;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class HepOpt {
  HepPlanner hepPlanner;
  RelToSqlConverter converter;
  Map<String, List<RelOptRule>> rule2ruleset = Map.of(
          "rule_agg", Arrays.<RelOptRule>asList(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,CoreRules.AGGREGATE_PROJECT_MERGE,CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,CoreRules.AGGREGATE_UNION_AGGREGATE,CoreRules.AGGREGATE_UNION_TRANSPOSE,CoreRules.AGGREGATE_VALUES, PruneEmptyRules.AGGREGATE_INSTANCE),
          "rule_filter",Arrays.<RelOptRule>asList(CoreRules.FILTER_AGGREGATE_TRANSPOSE,CoreRules.FILTER_CORRELATE,CoreRules.FILTER_INTO_JOIN,CoreRules.JOIN_CONDITION_PUSH,CoreRules.FILTER_MERGE,CoreRules.FILTER_MULTI_JOIN_MERGE, CoreRules.FILTER_PROJECT_TRANSPOSE,CoreRules.FILTER_SET_OP_TRANSPOSE,CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE,CoreRules.FILTER_SCAN,CoreRules.FILTER_REDUCE_EXPRESSIONS,CoreRules.PROJECT_REDUCE_EXPRESSIONS,PruneEmptyRules.FILTER_INSTANCE),
          "rule_join",Arrays.<RelOptRule>asList(CoreRules.JOIN_EXTRACT_FILTER,CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE,CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,CoreRules.JOIN_LEFT_UNION_TRANSPOSE,CoreRules.JOIN_RIGHT_UNION_TRANSPOSE,CoreRules.SEMI_JOIN_REMOVE,CoreRules.JOIN_REDUCE_EXPRESSIONS,CoreRules.PROJECT_REDUCE_EXPRESSIONS,PruneEmptyRules.JOIN_LEFT_INSTANCE,PruneEmptyRules.JOIN_RIGHT_INSTANCE)
  );
  //todo different rules
  public  HepOpt(){
    HepProgramBuilder builder = new HepProgramBuilder();
    this.hepPlanner = new HepPlanner(builder.addMatchOrder(HepMatchOrder.TOP_DOWN).build());
    //todo different dialects
    this.converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
  }

  public void updateRule(String rule){
    HepProgramBuilder builder = new HepProgramBuilder();
    for(RelOptRule rule_instance:this.rule2ruleset.get(rule)){
      builder.addRuleInstance(rule_instance);
    }
    this.hepPlanner = new HepPlanner(builder.addMatchOrder(HepMatchOrder.TOP_DOWN).build());
  }

  public List findBest(RelNode relNode){

    List res = new ArrayList();
    RelNode finalNode = relNode;
    for (int i = 0;i<5;i++){
      this.hepPlanner.setRoot(finalNode);
      finalNode = this.hepPlanner.findBestExp();
      System.out.println("RELNODE REWRITED");
      System.out.println(RelOptUtil.toString(finalNode));
    }
    res.add(finalNode);
    String new_sql = converter.visitRoot(finalNode).asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
    res.add(new_sql);
    return res;
  }
}