import com.alibaba.fastjson.JSONArray;
import main.DBConn;
import main.Rewriter;
import main.Utils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.util.Arrays;
import java.util.List;

public class test_single_rule {
    public static HepProgramBuilder getbuilder(RelOptRule rule_instance){
        HepProgramBuilder builder = new HepProgramBuilder();
        // RelOptRule rule_instance = CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES;
        builder.addRuleInstance(rule_instance);
        /*List<RelOptRule> t = Arrays.<RelOptRule>asList(CoreRules.AGGREGATE_PROJECT_MERGE,CoreRules.AGGREGATE_VALUES, PruneEmptyRules.AGGREGATE_INSTANCE);
        for(RelOptRule rule:t){
            builder.addRuleInstance(rule);
        }*/
        return builder;
    }
    public static void main(String[] args) throws Exception{

        String host = "166.111.121.55";
        String port = "15432";
        String user = "postgres";
        String passwd= "kDZCNgUV0zJwdq9";
        String dbname= "tpch10x";
        String dbDriver = "org.postgresql.Driver";
        DBConn db = new DBConn(host,port,user,passwd,dbname,dbDriver);
        Rewriter rewriter = new Rewriter(db);

        String testSql;

        testSql = "select l_discount,count (distinct l_orderkey), sum(distinct l_tax)\n" +
                "from lineitem, part\n" +
                "where l_discount > 100 group by l_discount;";

        testSql = testSql.replace(";", "");
        RelNode testRelNode = rewriter.SQL2RA(testSql);
        RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);

        RelOptRule ruleInstance = Utils.rule2RuleClass("AGGREGATE_JOIN_REMOVE");


        HepProgramBuilder builder = getbuilder(ruleInstance);
        HepPlanner hepPlanner = new HepPlanner(builder.addMatchOrder(HepMatchOrder.TOP_DOWN).build());
        hepPlanner.setRoot(testRelNode);
        RelNode rewrite_result = hepPlanner.findBestExp();
        System.out.println(testRelNode.explain());
        System.out.println(rewrite_result.explain());
        String rewrite_sql = converter.visitRoot(rewrite_result).asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
        if(rewrite_result.equals(testRelNode)){
            System.out.println("No changed!");
            return;
        }
        System.out.println(testSql);
        System.out.println(rewrite_sql);
    }
}
