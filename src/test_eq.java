import com.alibaba.fastjson.JSONArray;
import com.google.gson.JsonObject;
import main.Node;
import main.Rewriter;
import main.Utils;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import verify.*;
//
// import verify.*;
import org.apache.calcite.plan.RelOptUtil;

public class test_eq {
  public static void main(String[] args) throws Exception {

    //DB Config
    String path = System.getProperty("user.dir");
    JSONArray schemaJson = Utils.readJsonFile(path+"/src/main/schema.json");
    Rewriter rewriter = new Rewriter(schemaJson);


    //todo query formating
    String testSql = testSql = "select * from orders where (o_orderpriority + o_orderkey > 10 and o_orderkey < 100+2) and (1999 + 1 < o_totalprice and o_orderpriority like 'abcd') ";

    testSql = "SELECT\n" +
            "  *\n" +
            "from\n" +
            "  customer\n" +
            "where\n" +
            "  c_custkey > (\n" +
            "    SELECT\n" +
            "      MAX(l_orderkey)\n" +
            "    FROM\n" +
            "      lineitem\n" +
            "    where\n" +
            "      c_custkey = l_partkey\n" +
            "  )" +
            "order by c_custkey;";
    testSql = "(select * from lineitem order by l_orderkey) union (select * from lineitem order by l_partkey)";
    testSql = "SELECT\n" +
            "  *\n" +
            "from\n" +
            "  customer\n" +
            "where\n" +
            "  c_custkey > (\n" +
            "    SELECT\n" +
            "      MAX(l_orderkey)\n" +
            "    FROM\n" +
            "      lineitem\n" +
            "    where\n" +
            "      c_custkey = l_partkey and True\n" +
            "  );";
    testSql = "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 313 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate LIMIT 5;";
    String sql2 = "SELECT \"t157\".\"c_name\", \"t157\".\"c_custkey\", \"t157\".\"o_orderkey\", \"t157\".\"o_orderdate\", \"t157\".\"o_totalprice\", \"t157\".\"$f8\" FROM ( SELECT \"t155\".\"c_custkey\", \"t155\".\"c_name\", \"t155\".\"o_orderkey\", \"t155\".\"o_totalprice\", \"t155\".\"o_orderdate\", \"t155\".\"$f8\" * \"t156\".\"EXPR$5\" AS \"$f8\" FROM ( SELECT \"t153\".\"c_custkey\", \"t153\".\"c_name\", \"t154\".\"o_orderkey\", \"t154\".\"o_totalprice\", \"t154\".\"o_orderdate\", \"t153\".\"$f2\" * \"t154\".\"$f4\" AS \"$f8\" FROM ( SELECT \"c_custkey\", \"c_name\", COUNT(*) AS \"$f2\" FROM \"customer\" GROUP BY \"c_custkey\", \"c_name\" ) AS \"t153\" INNER JOIN ( SELECT \"o_orderkey\", \"o_custkey\", \"o_totalprice\", \"o_orderdate\", COUNT(*) AS \"$f4\" FROM \"orders\" GROUP BY \"o_orderkey\", \"o_custkey\", \"o_totalprice\", \"o_orderdate\" ) AS \"t154\" ON \"t153\".\"c_custkey\" = \"t154\".\"o_custkey\" ) AS \"t155\" INNER JOIN ( SELECT \"l_orderkey\", SUM(\"l_quantity\") AS \"EXPR$5\" FROM \"lineitem\" GROUP BY \"l_orderkey\" ) AS \"t156\" ON \"t155\".\"o_orderkey\" = \"t156\".\"l_orderkey\" ) AS \"t157\" INNER JOIN ( SELECT \"l_orderkey\" FROM \"lineitem\" GROUP BY \"l_orderkey\" HAVING SUM(\"l_quantity\") > 313 ) AS \"t160\" ON \"t157\".\"o_orderkey\" = \"t160\".\"l_orderkey\" ORDER BY \"t157\".\"o_totalprice\" DESC, \"t157\".\"o_orderdate\" FETCH NEXT 5 ROWS ONLY;";
    // 使用方法：修改testSql与sql2。
    // testSql = "select distinct c1.c_custkey as ck from customer c1, customer c2, orders o where c1.c_custkey = c2.c_custkey and c1.c_custkey = o.o_orderkey";
    testSql = testSql.replace(";", "");
    sql2 = sql2.replace(";","");
    RelNode testRelNode = rewriter.SQL2RA(testSql);
    double origin_cost = rewriter.getCostRecordFromRelNode(testRelNode);
    Node resultNode = new Node(testSql,testRelNode, (float) origin_cost,rewriter, (float) 0.1,null,"original query");
    Node res = resultNode.UTCSEARCH(5, resultNode,1);
    // System.out.println(RelOptUtil.toString(rewriter.removeOrderbyNCalc(testRelNode,null,0)));
    System.out.println("--------Equality Check: Two Relnodes: -------------");
    RelNode testRelNode_v = rewriter.removeOrderbyNCalc(testRelNode,null,0);
    RelNode rewriteRelNode_v = rewriter.removeOrderbyNCalc(testRelNode,null,0);

    RelNode sql2node = rewriter.removeOrderbyNCalc(rewriter.SQL2RA(sql2),null,0);
    JsonObject eqres = verifyrelnode.verifyrelnode(testRelNode_v, sql2node, testSql, res.state);
    System.out.println("-------Equality Check Res: --------------");
    System.out.println(eqres);
    // EquivCheck.checkeq(rewriter, testSql, res.state, testRelNode, res.state_rel);
  }
}
