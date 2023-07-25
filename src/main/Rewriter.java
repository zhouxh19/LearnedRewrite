package main;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
// **** 规则添加
import org.apache.calcite.rel.core.*;
// *** 规则结束
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.SourceStringReader;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.avatica.util.Casing.UNCHANGED;
import static org.apache.calcite.avatica.util.Quoting.DOUBLE_QUOTE;

public class Rewriter {
  public DBConn db;
  public ArrayList tableList;
  public Vector<Pair<String, Vector<Pair<String, String>>>> schema;
  Planner planner;
  SqlDialect dialect;
  public HepOpt optimizer;

  Map<String, List<RelOptRule>> rule2ruleset = Map.of(
          "rule_agg", Arrays.<RelOptRule>asList(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,CoreRules.AGGREGATE_PROJECT_MERGE,CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,CoreRules.AGGREGATE_UNION_AGGREGATE,CoreRules.AGGREGATE_UNION_TRANSPOSE,CoreRules.AGGREGATE_VALUES, PruneEmptyRules.AGGREGATE_INSTANCE),
          "rule_filter",Arrays.<RelOptRule>asList(CoreRules.FILTER_AGGREGATE_TRANSPOSE,CoreRules.FILTER_CORRELATE,CoreRules.FILTER_INTO_JOIN,CoreRules.JOIN_CONDITION_PUSH,CoreRules.FILTER_MERGE,CoreRules.FILTER_MULTI_JOIN_MERGE, CoreRules.FILTER_PROJECT_TRANSPOSE,CoreRules.FILTER_SET_OP_TRANSPOSE,CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE,CoreRules.FILTER_SCAN,CoreRules.FILTER_REDUCE_EXPRESSIONS,CoreRules.PROJECT_REDUCE_EXPRESSIONS,PruneEmptyRules.FILTER_INSTANCE),
          "rule_join",Arrays.<RelOptRule>asList(CoreRules.JOIN_EXTRACT_FILTER,CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE,CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,CoreRules.JOIN_LEFT_UNION_TRANSPOSE,CoreRules.JOIN_RIGHT_UNION_TRANSPOSE,CoreRules.SEMI_JOIN_REMOVE,CoreRules.JOIN_REDUCE_EXPRESSIONS,PruneEmptyRules.JOIN_LEFT_INSTANCE,PruneEmptyRules.JOIN_RIGHT_INSTANCE),
          "rule_project",Arrays.<RelOptRule>asList(CoreRules.PROJECT_CALC_MERGE,CoreRules.PROJECT_CORRELATE_TRANSPOSE,CoreRules.PROJECT_MERGE,CoreRules.PROJECT_MULTI_JOIN_MERGE, CoreRules.PROJECT_REMOVE,CoreRules.PROJECT_TO_CALC,CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,CoreRules.PROJECT_REDUCE_EXPRESSIONS,PruneEmptyRules.PROJECT_INSTANCE),
          "rule_cal",Arrays.<RelOptRule>asList(CoreRules.CALC_MERGE,CoreRules.CALC_REMOVE),
          "rule_orderby",Arrays.<RelOptRule>asList(CoreRules.SORT_JOIN_TRANSPOSE,CoreRules.SORT_PROJECT_TRANSPOSE,CoreRules.SORT_UNION_TRANSPOSE,CoreRules.SORT_REMOVE_CONSTANT_KEYS,CoreRules.SORT_REMOVE,PruneEmptyRules.SORT_INSTANCE,PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE),
          "rule_union",Arrays.<RelOptRule>asList(CoreRules.UNION_MERGE,CoreRules.UNION_REMOVE,CoreRules.UNION_TO_DISTINCT,CoreRules.UNION_PULL_UP_CONSTANTS,PruneEmptyRules.UNION_INSTANCE,PruneEmptyRules.INTERSECT_INSTANCE,PruneEmptyRules.MINUS_INSTANCE)
  );

  public Map<String, List<String>> rulegroup2rules = Map.of("rule_filter", Arrays.<String>asList("FILTER_AGGREGATE_TRANSPOSE", "FILTER_CORRELATE", "FILTER_INTO_JOIN", "JOIN_CONDITION_PUSH", "FILTER_MERGE", "FILTER_MULTI_JOIN_MERGE", "FILTER_PROJECT_TRANSPOSE", "FILTER_SET_OP_TRANSPOSE", "FILTER_TABLE_FUNCTION_TRANSPOSE", "FILTER_SCAN", "FILTER_REDUCE_EXPRESSIONS", "PROJECT_REDUCE_EXPRESSIONS"),
          "rule_join", Arrays.<String>asList("JOIN_EXTRACT_FILTER", "JOIN_PROJECT_BOTH_TRANSPOSE", "JOIN_PROJECT_LEFT_TRANSPOSE", "JOIN_PROJECT_RIGHT_TRANSPOSE", "JOIN_LEFT_UNION_TRANSPOSE", "JOIN_RIGHT_UNION_TRANSPOSE", "SEMI_JOIN_REMOVE", "JOIN_REDUCE_EXPRESSIONS", "JOIN_LEFT_INSTANCE", "JOIN_RIGHT_INSTANCE"),
          "rule_agg", Arrays.<String>asList("AGGREGATE_EXPAND_DISTINCT_AGGREGATES","AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN","AGGREGATE_JOIN_TRANSPOSE_EXTENDED","AGGREGATE_PROJECT_MERGE","AGGREGATE_ANY_PULL_UP_CONSTANTS", "AGGREGATE_UNION_AGGREGATE", "AGGREGATE_UNION_TRANSPOSE", "AGGREGATE_VALUES"),
          "rule_project",Arrays.<java.lang.String>asList("PROJECT_CALC_MERGE", "PROJECT_CORRELATE_TRANSPOSE", "PROJECT_MERGE", "PROJECT_MULTI_JOIN_MERGE", "PROJECT_REMOVE", "PROJECT_TO_CALC", "PROJECT_SUB_QUERY_TO_CORRELATE", "PROJECT_REDUCE_EXPRESSIONS", "PruneEmptyRules", "PROJECT_INSTANCE"),
          "rule_cal",Arrays.<java.lang.String>asList("CALC_MERGE", "CALC_REMOVE"),
          "rule_orderby",Arrays.<java.lang.String>asList("SORT_JOIN_TRANSPOSE", "SORT_PROJECT_TRANSPOSE", "SORT_UNION_TRANSPOSE", "SORT_REMOVE_CONSTANT_KEYS", "SORT_REMOVE", "SORT_INSTANCE", "SORT_FETCH_ZERO_INSTANCE"),
          "rule_union",Arrays.<java.lang.String>asList("UNION_MERGE", "UNION_REMOVE", "UNION_TO_DISTINCT", "UNION_PULL_UP_CONSTANTS", "UNION_INSTANCE", "INTERSECT_INSTANCE", "MINUS_INSTANCE")
  );

  public Map<String,Class> rule2class = Map.of("rule_filter", Filter.class,
      "rule_join", Join.class,
      "rule_agg", Aggregate.class,
      "rule_project", Project.class,
      "rule_cal", Calc.class,
      "rule_orderby", Sort.class,
      "rule_union", Union.class
  );
  public List<String> rule_list = Arrays.asList("rule_agg","rule_filter","rule_join","rule_project","rule_cal","rule_orderby","rule_union");

  public  Rewriter(DBConn db) throws SQLException, IOException {
    this.db = db;

    Vector<Pair<String, Vector<Pair<String, String>>>> schema_all = new Vector<>();
    SchemaPlus rootSchema = GenerateSchema.generate_schema(this.db.getSchema(), schema_all);

    SqlParser.Config parserConfig = SqlParser.config().withLex(Lex.MYSQL).withUnquotedCasing(UNCHANGED).withCaseSensitive(false).withQuoting(DOUBLE_QUOTE).withConformance(SqlConformanceEnum.MYSQL_5);
    FrameworkConfig config = Frameworks.newConfigBuilder().parserConfig(parserConfig).defaultSchema(rootSchema).build();
    this.planner = Frameworks.getPlanner(config);
    this.schema = schema_all;
    this.dialect = PostgresqlSqlDialect.DEFAULT;
    this.optimizer = new HepOpt();
  }

  public RelOptCost getCostFromRelNode(RelNode rel_node) throws Exception {


    Deque stack = new LinkedList();
    stack.add(rel_node);
    RelOptCost tol_cost = rel_node.computeSelfCost(rel_node.getCluster().getPlanner(),rel_node.getCluster().getMetadataQuery());
    while (stack.size()>0) {
      RelNode node = (RelNode) stack.pop();
      tol_cost = tol_cost.plus(node.computeSelfCost(node.getCluster().getPlanner(),node.getCluster().getMetadataQuery()));
      for (int i = 0; i < node.getInputs().size(); i++) {
        stack.add(node.getInputs().get(i));
      }
    }
    return tol_cost;
  }

  public double getCostRecordFromRelNode(RelNode rel_node) throws Exception {
    return getCostFromRelNode(rel_node).getRows()+getCostFromRelNode(rel_node).getCpu()*0.01+getCostFromRelNode(rel_node).getIo()*4; // these metrics should have been obtained from db!!
  }
  //remove useless nodes before verify
  public RelNode removeOrderbyNCalc(RelNode rel_node,RelNode parent,int childIndex) {
    for(int i = 0;i<rel_node.getInputs().size();i++){
      removeOrderbyNCalc(rel_node.getInput(i),rel_node,i);
    }
    if (rel_node instanceof LogicalSort || rel_node instanceof  LogicalCalc){
      if (parent == null){
        rel_node = rel_node.getInput(0);
      }
      else {
        parent.replaceInput(childIndex,rel_node.getInput(0));
      }
    }
    return rel_node;
  }

  // remove useless aggregate
  public RelNode rel_formatting(RelNode rel_node){
    Deque stack = new LinkedList();
    stack.add(rel_node);
    while (stack.size()>0) {
      RelNode node = (RelNode) stack.pop();
      for (int i = 0; i < node.getInputs().size(); i++) {
        stack.add(node.getInputs().get(i));
        RelNode child = node.getInput(i);
        if (child instanceof LogicalAggregate){
          if(((LogicalAggregate) child).getAggCallList().size() == 0){
            node.replaceInput(i,child.getInput(0));
          } else if (((LogicalAggregate) child).getAggCallList().get(0).toString().contains("SINGLE_VALUE")){
            node.replaceInput(i,child.getInput(0));
          } else if (((LogicalAggregate) child).getAggCallList().get(0).toString().contains("MIN")){
            RelDataType records = child.getRowType();
            List column_names = records.getFieldNames();
            List column_types = RelOptUtil.getFieldTypeList(records);
            int flag =  ((LogicalAggregate) child).getAggCallList().get(0).getArgList().get(0);
            if((((String) column_names.get(flag)).contains("$")) && ((column_types.get(flag).toString()).contains("BOOLEAN"))){
              node.replaceInput(i,child.getInput(0));
            }
            RelNode tmp = child.getInput(0);
          }
        }
      }
    }
    return rel_node;
  }

  //String sql to RelNode
  public RelNode SQL2RA(String sql) throws SqlParseException, ValidationException, RelConversionException {
    this.planner.close();
    this.planner.reset();

    sql = sql.replace(";", "");
    sql = sql.replace("!=", "<>");

    SqlNode sql_node = this.planner.parse(new SourceStringReader(sql));
    sql_node = this.planner.validate(sql_node);

    RelRoot rel_root = this.planner.rel(sql_node);
    RelNode rel_node = rel_root.project();
//    rel_node = rel_formatting(rel_node);
    //System.out.println("RELNODE PLAIN");
    //System.out.println(RelOptUtil.toString(rel_node));
    //System.exit(0);
    return rel_node;
  }

  //RBO optimizing with HepPlanner
  public List singleRewrite(RelNode relNode, String rule) {
    //todo rule selection
    this.optimizer.updateRule(rule);
    List res = this.optimizer.findBest(relNode);
    //todo is_rewrite
    res.add(1);
    return res;
  }

  static String get_relative_columns (List<RelNode> childs, int index){
    String res = "";
    if (index<childs.get(0).getRowType().getFieldNames().size()){
      if(childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index) != null){
        res += childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index).getOriginTable().getQualifiedName().get(0);
        res += "."+childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index).getOriginTable().getRowType().getFieldNames().get(childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index).getOriginColumnOrdinal());
      }
    }
    else {
      index-=childs.get(0).getRowType().getFieldNames().size();
      if(childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1), index) != null){
        res += childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1),index).getOriginTable().getQualifiedName().get(0);
        res += "."+childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1),index).getOriginTable().getRowType().getFieldNames().get(childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1),index).getOriginColumnOrdinal());
      }
    }
    return res;
  }

  public static JSONObject getRelNodeTreeJson(RelNode node){
    JSONObject tree = new JSONObject();
    HashSet<String> column_res = new HashSet<>();
    List<JSONObject> subtree = new ArrayList<>();
    String pattern = "[\\[( =]\\$(\\d+)";
    HashSet<String> table_res = new HashSet<>();


    Pattern r = Pattern.compile(pattern);
    List<RelNode> children = node.getInputs();
    Matcher m = r.matcher(node.toString());


    while (m.find()){
      int index = Integer.parseInt(m.group().substring(2));
      String column_name = get_relative_columns(children, index);
      if (column_name!=""){
        column_res.add(column_name);
      }
    }


    if (node instanceof TableScan){
      String table_name = node.getTable().getQualifiedName().get(0);
      table_res.add(table_name);
    }

    String[] node_name = node.getClass().toString().split("\\.");
    tree.put("node",node_name[node_name.length-1]);
    tree.put("columns:",column_res);
    tree.put("tables",table_res);

    for(RelNode child :children){
      subtree.add(getRelNodeTreeJson(child));
    }
    tree.put("children", subtree);
    return tree;
  }

}


