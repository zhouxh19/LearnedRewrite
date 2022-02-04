package main;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
// **** 规则添加
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
// *** 规则结束
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.SourceStringReader;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.avatica.util.Casing.UNCHANGED;
import static org.apache.calcite.avatica.util.Quoting.DOUBLE_QUOTE;


public class Rewriter {
  public DBConn db;
  public ArrayList tableList;
  Planner planner;
  SqlDialect dialect;
  public HepOpt optimizer;

  public Map<String,Class> rule2class = Map.of("rule_filter", Filter.class,"rule_join", Join.class,"rule_agg", Aggregate.class);

  public  Rewriter(String host,String port,String driver,String user,String password,String dbname,DBConn db) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calcite_conn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calcite_conn.getRootSchema();
    DataSource data_source = JdbcSchema.dataSource("jdbc:postgresql://"+host+':'+port+'/',
            driver, user,password);
    rootSchema.add(dbname, JdbcSchema.create(rootSchema,dbname, data_source, null, null));

    System.out.println("----------Schema----------");
    System.out.println(rootSchema.getTableNames());
    SqlParser.Config parserConfig = SqlParser.config().withLex(Lex.MYSQL).withUnquotedCasing(UNCHANGED).withCaseSensitive(false).withQuoting(DOUBLE_QUOTE);
    FrameworkConfig config = Frameworks.newConfigBuilder().parserConfig(parserConfig).defaultSchema(rootSchema).build();
    //SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
    this.planner = Frameworks.getPlanner(config);
    this.dialect = PostgresqlSqlDialect.DEFAULT;
    this.db = db;
    this.tableList = db.getTableName();
    this.optimizer = new HepOpt();
  }

  //remove useless aggregate
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



    SqlNode sql_node = this.planner.parse(new SourceStringReader(sql));
    sql_node = this.planner.validate(sql_node);

    RelRoot rel_root = this.planner.rel(sql_node);
    RelNode rel_node = rel_root.project();
    rel_node = rel_formatting(rel_node);
    System.out.println("RELNODE PLAIN");
    System.out.println(RelOptUtil.toString(rel_node));


    return rel_node;
  }

  //RBO optimizing with HepPlanner
  public List singleRewrite(String sql, String rule) throws ValidationException, SqlParseException, RelConversionException {
    //todo rule selection
    String formatted_sql = new Utils().sqlFormattingToCalcite(sql, this.db.dbname, this.tableList);
    this.optimizer.updateRule(rule);
    RelNode relNode = this.SQL2RA(formatted_sql);
    List res = this.optimizer.findBest(relNode);
    //todo is_rewrite
    res.add(1);
    return res;

  }

}


