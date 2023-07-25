import com.alibaba.fastjson.JSONArray;
import main.Node;
import main.Rewriter;
import main.Utils;
import org.apache.calcite.rel.RelNode;



public class test {
  public static void main(String[] args) throws Exception {

    //Config
    String path = System.getProperty("user.dir");
    JSONArray schemaJson = Utils.readJsonFile(path+"/src/main/schema.json");
    Rewriter rewriter = new Rewriter(schemaJson);

    //todo query formating
    String testSql = "select * from orders where (o_orderpriority + o_orderkey > 10 and o_orderkey < 100+2) and (1999 + 1 < o_totalprice and o_orderpriority like 'abcd')";

    RelNode testRelNode = rewriter.SQL2RA(testSql);
    double origin_cost = rewriter.getCostRecordFromRelNode(testRelNode);

    Node resultNode = new Node(testSql,testRelNode, (float) origin_cost,rewriter, (float) 0.1,null,"original query");

    Node res = resultNode.UTCSEARCH(20, resultNode,1);
    System.out.println(testSql);
    System.out.println("root:"+res.state);
    System.out.println("Original cost: "+origin_cost);
    System.out.println("Optimized cost: "+rewriter.getCostRecordFromRelNode(res.state_rel));
    System.out.println(Utils.generate_json(resultNode));
  }
}
