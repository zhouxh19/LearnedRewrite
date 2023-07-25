import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import main.DBConn;
import main.Node;
import main.Rewriter;
import main.Utils;
import org.apache.calcite.rel.RelNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;

import org.apache.commons.lang.StringEscapeUtils;

public class test_workload {
    public static void main(String[] args) throws Exception {

        //DB Config
        String host = "166.111.121.55";
        String port = "15432";
        String user = "postgres";
        String passwd= "kDZCNgUV0zJwdq9";
        String dbname= "tpch10x";
        String dbDriver = "org.postgresql.Driver";
        String db_dir = args[0];

        System.out.println("=======:" + args[0]);
        File directory = new File(db_dir);
        String dbname2 = dbname;
        System.out.println("=======:" + directory.getPath()+"/tpch10000.txt");
        DBConn db = new DBConn(host,port,user,passwd,dbname2,dbDriver);
        Rewriter rewriter = new Rewriter(db);
        String[] workload = Utils.readWorkloadFromFile(String.valueOf(directory.getPath()+"/tpch10000.txt"));
        //System.out.println(workload[0]);
        System.out.println(workload.length);
        List addedWorkload = new ArrayList();
        JSONArray rewrittenList = new JSONArray();
        List unRewrittenList = new ArrayList();
        List failureList = new ArrayList();

        for (int i = 0; i < workload.length; i++) {
            String sql = workload[i];
            if (addedWorkload.contains(sql)) {
                continue;
            }
            System.out.println("\u001B[1;31m" + "-------------------------------------------正在改写："+ i + "\u001B[0m");
            try {
                RelNode originRelNode = rewriter.SQL2RA(sql);
                //double origin_cost = rewriter.getCostRecordFromRelNode(originRelNode);
                double origin_cost = rewriter.db.getCost(sql);
                Node resultNode = new Node(sql, originRelNode, (float) origin_cost, rewriter, (float) 0.1,null,"");
                Node res = resultNode.UTCSEARCH(20, resultNode,1);
                String rewritten_sql = res.state;
                //if (!rewritten_sql.equalsIgnoreCase(sql)) {
                //if (origin_cost > rewriter.db.getCost(res.state)) {
                if (origin_cost>=0) {
                    JSONObject dataJson = new JSONObject();
                    dataJson.put("origin_cost", String.format("%.4f",origin_cost));
                    System.out.println("origin_cost: " + String.format("%.4f",origin_cost));
                    dataJson.put("origin_sql", sql);
                    System.out.println("origin_rel" + resultNode.state_rel);
                    dataJson.put("rewritten_cost", String.format("%.4f",rewriter.db.getCost(res.state)));
                    System.out.println("rewritten_cost: " + String.format("%.4f",rewriter.db.getCost(res.state)));
                    dataJson.put("rewritten_sql", res.state);
                    System.out.println("rewritten_rel" + res.state_rel);
                    System.out.println("rule_sequence: " + res.rewrite_sequence.toString());
                    dataJson.put("rule_sequence", res.rewrite_sequence.toString());
                    rewrittenList.add(dataJson);
                }else {
                    unRewrittenList.add(sql);
                }
            } catch (Exception error) {
                System.out.println(error.toString());
                failureList.add(sql);
            }
            addedWorkload.add(sql);
        }



    }
}
