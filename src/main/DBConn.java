package main;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//import org.postgresql.jdbc.;


public class DBConn {
  private String host;
  private String port;
  private String user;
  private String password;
  public String dbname;
  private Connection conn;
  private String DBDriver;

  public DBConn(String host, String port, String user, String password, String dbname, String DBDriver)
          throws Exception {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.dbname = dbname;
    this.DBDriver = DBDriver;
    get_conn();
  }

  private void get_conn() throws Exception{
    Class.forName(this.DBDriver);
    //Class.forName("org.postgresql.Driver");
    this.conn = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + dbname, user, password);
  }

  public ArrayList<String> getTables() {
    ArrayList list = new ArrayList();
    try {
      Statement stmt = this.conn.createStatement();
      boolean success = stmt.execute("SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;");
      if (success){
        ResultSet res = stmt.getResultSet();
        while(res.next()){
          String s = res.getString(1);
          list.add(s);
        }
        System.out.println("获取table成功: " + list);
        stmt.close();
      } else {
        System.out.println("获取table name失败");
      }
    } catch (SQLException e) {
      System.out.println("获取table name失败：" + e);
    }
    return list;
  }

  public JSONArray getColumns(String tableName) {
    JSONArray list = new JSONArray();
    try {
      Statement stmt = this.conn.createStatement();
      boolean success = stmt.execute("select column_name, data_type from information_schema.columns where table_name = '" + tableName + "';");
      if (success){
        ResultSet res = stmt.getResultSet();
        while(res.next()){
          JSONObject map = new JSONObject();
          map.put("name", res.getString(1));
          map.put("type", res.getString(2));
          list.add(map);
        }
        stmt.close();
      } else {
        System.out.println("获取Column失败");
      }
    } catch (SQLException e) {
      System.out.println("获取Column失败：" + e);
    }
    return list;
  }

  public JSONArray getSchema() {
    JSONArray schema = new JSONArray();
    ArrayList<String> tables = this.getTables();

    for (String tableName : tables) {
      JSONObject map = new JSONObject();
      map.put("table", tableName);
      map.put("rows", 0);
      map.put("columns", this.getColumns(tableName));
      schema.add(map);
    }
    System.out.println("Schmea:" + schema.toString());
    return schema;
  }

  public float getCost(String sql) {
//    todo 处理请求异常
    float cost = -1;
    try {
      Statement stmt = this.conn.createStatement();
      //System.out.println("getting cost for sql:");
      // System.out.println(sql);
      boolean success = stmt.execute("explain (FORMAT JSON)" + sql);
      if (success){
        ResultSet res = stmt.getResultSet();
        res.next();
        String s = res.getArray(1).toString().strip();
        JSONArray jarray = (JSONArray) JSONObject.parse(s);
        JSONObject jobject = (JSONObject) jarray.get(0);
        jobject = (JSONObject) jobject.get("Plan");
        cost = ((BigDecimal) jobject.get("Total Cost")).floatValue();
        stmt.close();
      } else {
        System.out.println("获取Cost失败：" + sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      cost = -1;
    }
    return cost;
  }
}