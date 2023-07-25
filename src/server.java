import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.InetSocketAddress;

import main.DBConn;
import main.Node;
import main.Utils;
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
import org.apache.commons.io.IOUtils;
import java.text.DateFormat;
import java.util.Date;
import main.Rewriter;

public class server {
    public static void main(String[] arg) throws Exception {
        Integer port = 5444;
        if (arg.length > 0) {
            port = Integer.parseInt(arg[0]);
        }
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/rewrite", new RewriteHandler());
        server.createContext("/rewrite/single_rule", new RewriteSingleRuleHandler());
        server.createContext("/parser", new ParserHandler());
        server.start();
        System.out.println("服务启动成功，端口号为：" + port);
    }

    static class RewriteSingleRuleHandler implements HttpHandler{
        public void handle(HttpExchange exchange) throws IOException {

            new Thread(new Runnable() {
                @Override
                public void run() {
                    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

                    if (exchange.getRequestMethod().equalsIgnoreCase("OPTIONS")) {
                        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS, POST");
                        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type,Authorization");
                        try {
                            exchange.sendResponseHeaders(204, -1);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return;
                    }

                    JSONObject responseJson = new JSONObject();

                    String logString = DateFormat.getInstance().format(new Date()).toString();
                    logString += "\n";
                    try{
                        //获得表单提交数据(post)
                        String postString = IOUtils.toString(exchange.getRequestBody());
                        System.out.println("postString:" + postString);
                        logString += postString;
                        logString += "\n";
                        JSONObject postInfo = JSONObject.parseObject(postString);
                        String sql = postInfo.getString("sql");
                        String rule = postInfo.getString("rule");

                        if (sql == null) {
                            responseJson.put("status", false);
                            responseJson.put("message", "Please enter Sql");
                        } else if (rule == null) {
                            responseJson.put("status", false);
                            responseJson.put("message", "Please enter Rule");
                        } else {
                            String host = "182.92.xxx.xx";
                            String port = "5432";
                            String user = "name";
                            String passwd= "password";
                            String dbname= "tpch";
                            String dbDriver = "org.postgresql.Driver";
                            DBConn db = new DBConn(host,port,user,passwd,dbname,dbDriver);
                            Rewriter rewriter = new Rewriter(db);
                            RelNode relNode = rewriter.SQL2RA(sql);

                            RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
                            RelOptRule ruleInstance = Utils.rule2RuleClass(rule);
                            HepProgramBuilder builder = new HepProgramBuilder();
                            builder.addRuleInstance(ruleInstance);
                            HepPlanner hepPlanner = new HepPlanner(builder.addMatchOrder(HepMatchOrder.TOP_DOWN).build());
                            hepPlanner.setRoot(relNode);
                            RelNode rewrite_result = hepPlanner.findBestExp();
                            String rewrite_sql = converter.visitRoot(rewrite_result).asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();

                            double origin_cost = rewriter.db.getCost(sql);
                            JSONObject dataJson = new JSONObject();
                            dataJson.put("origin_cost", String.format("%.4f",origin_cost));
                            dataJson.put("origin_sql", sql);
                            dataJson.put("origin_sql_node", RelOptUtil.toString(relNode));
                            dataJson.put("rewritten_cost", String.format("%.4f",rewriter.db.getCost(rewrite_sql)));
                            dataJson.put("rewritten_sql", rewrite_sql);
                            dataJson.put("rewritten_sql_node", RelOptUtil.toString(rewrite_result));
                            dataJson.put("is_rewritten", !rewrite_sql.equalsIgnoreCase(sql));
                            responseJson.put("status", true);
                            responseJson.put("message", "SUCCESS");
                            responseJson.put("data", dataJson);
                            logString += responseJson.toJSONString();
                            logString += "\n";
                            logString += DateFormat.getInstance().format(new Date()).toString();
                            logString += "\n";
                            logString += "====================================================\n\n";
                        }
                    } catch (Exception e) {
                        System.out.println(e);
                        responseJson.put("status", false);
                        responseJson.put("message", "Get Error");
                        logString += e.toString();
                        logString += "\n";
                        logString += "====================================================\n\n";
                    } finally {
                        try{
                            OutputStream os = exchange.getResponseBody();
                            exchange.sendResponseHeaders(200,0);
                            os.write(responseJson.toJSONString().getBytes());
                            os.close();
                            FileOutputStream o = null;
                            File file = new File("request.txt");
                            if(!file.exists()){
                                file.createNewFile();
                            }
                            byte[] buff = new byte[]{};
                            buff=logString.getBytes();
                            o=new FileOutputStream(file,true);
                            o.write(buff);
                            o.flush();
                            o.close();
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
    }

    static class RewriteHandler implements HttpHandler{
        public void handle(HttpExchange exchange) throws IOException {

            new Thread(new Runnable() {
                @Override
                public void run() {
                    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

                    if (exchange.getRequestMethod().equalsIgnoreCase("OPTIONS")) {
                        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS, POST");
                        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type,Authorization");
                        try {
                            exchange.sendResponseHeaders(204, -1);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return;
                    }

                    JSONObject responseJson = new JSONObject();

                    String logString = DateFormat.getInstance().format(new Date()).toString();
                    logString += "\n";
                    try{
                        //获得表单提交数据(post)
                        String postString = IOUtils.toString(exchange.getRequestBody());
                        System.out.println("postString:" + postString);
                        logString += postString;
                        logString += "\n";
                        JSONObject postInfo = JSONObject.parseObject(postString);
                        String sql = postInfo.getString("sql");
                        String schemaJson = postInfo.getString("schema");

                        if (sql == null) {
                            responseJson.put("status", false);
                            responseJson.put("message", "Please enter Sql");
                        }else {

                            String host = "182.92.xxx.xx";
                            String port = "5432";
                            String user = "name";
                            String passwd= "password";
                            String dbname= "tpch";
                            String dbDriver = "org.postgresql.Driver";

                            DBConn db = new DBConn(host,port,user,passwd,dbname,dbDriver);
                            Rewriter rewriter = new Rewriter(db);
                            RelNode relNode = rewriter.SQL2RA(sql);
                            double origin_cost = rewriter.db.getCost(sql);
                            Node resultNode = new Node(sql, relNode, (float) origin_cost, rewriter, (float) 0.1,null,"");
                            System.out.println("============" + resultNode.toString());
                            Node res = resultNode.UTCSEARCH(20, resultNode,1);
                            JSONObject dataJson = new JSONObject();
                            JSONObject treeJson = Utils.generate_json(resultNode);
                            dataJson.put("origin_cost", String.format("%.4f",origin_cost));
                            dataJson.put("origin_sql", sql);
                            dataJson.put("origin_sql_node", RelOptUtil.toString(relNode));
                            dataJson.put("rewritten_cost", String.format("%.4f",rewriter.db.getCost(res.state)));
                            dataJson.put("rewritten_sql", res.state);
                            dataJson.put("rewritten_sql_node", RelOptUtil.toString(res.state_rel));
                            dataJson.put("is_rewritten", !res.state.equalsIgnoreCase(sql));
                            dataJson.put("treeJson", treeJson);
                            responseJson.put("status", true);
                            responseJson.put("message", "SUCCESS");
                            responseJson.put("data", dataJson);
                            logString += responseJson.toJSONString();
                            logString += "\n";
                            logString += DateFormat.getInstance().format(new Date()).toString();
                            logString += "\n";
                            logString += "====================================================\n\n";
                        }
                    } catch (Exception e) {
                        System.out.println(e);
                        responseJson.put("status", false);
                        responseJson.put("message", "Get Error");
                        logString += e.toString();
                        logString += "\n";
                        logString += "====================================================\n\n";
                    } finally {
                        try{
                            OutputStream os = exchange.getResponseBody();
                            exchange.sendResponseHeaders(200,0);
                            os.write(responseJson.toJSONString().getBytes());
                            os.close();
                            FileOutputStream o = null;
                            File file = new File("request.txt");
                            if(!file.exists()){
                                file.createNewFile();
                            }
                            byte[] buff = new byte[]{};
                            buff=logString.getBytes();
                            o=new FileOutputStream(file,true);
                            o.write(buff);
                            o.flush();
                            o.close();
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
    }

    static class ParserHandler implements HttpHandler{
        public void handle(HttpExchange exchange) throws IOException {

            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

            if (exchange.getRequestMethod().equalsIgnoreCase("OPTIONS")) {
                exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS, POST");
                exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type,Authorization");
                try {
                    exchange.sendResponseHeaders(204, -1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return;
            }

            JSONObject responseJson = new JSONObject();
            try{
                //获得表单提交数据(post)
                String postString = IOUtils.toString(exchange.getRequestBody());
                JSONObject postInfo = JSONObject.parseObject(postString);
                String sql = postInfo.getString("sql");

                System.out.println("请求sql：" + sql);
                if (sql == null) {
                    responseJson.put("status", false);
                    responseJson.put("message", "Please enter Sql");
                } else {

                    String host = "182.92.xxx.xx";
                    String port = "5432";
                    String user = "name";
                    String passwd= "password";
                    String dbname= "tpch";
                    String dbDriver = "org.postgresql.Driver";
                    DBConn db = new DBConn(host,port,user,passwd,dbname,dbDriver);

                    Rewriter rewriter = new Rewriter(db);
                    RelNode relNode = rewriter.SQL2RA(sql);
                    JSONObject dataJson = new JSONObject();
                    dataJson.put("res_node", RelOptUtil.toString(relNode));
                    responseJson.put("status", true);
                    responseJson.put("message", "SUCCESS");
                    responseJson.put("data", dataJson);
                }
            } catch (Exception e) {
                responseJson.put("status", false);
                responseJson.put("message", "Get Error");
            } finally {
                OutputStream os = exchange.getResponseBody();
                exchange.sendResponseHeaders(200,0);
                os.write(responseJson.toJSONString().getBytes());
                os.close();
            }
        }
    }

}
