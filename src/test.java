import main.DBConn;
import main.Node;
import main.Rewriter;
import main.Utils;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;

public class test {
  public static void main(String[] args) throws Exception {

    //DB Config
    String host = "123.56.63.105";
    String port = "5432";
    String user = "tpch";
    String passwd= "hello_tpch";
    String dbname = "tpch";
    String dbDriver = "org.postgresql.Driver";

    DBConn db = new DBConn(host,port,user,passwd,dbname,dbDriver);
    Rewriter rewriter = new Rewriter(host,port,dbDriver,user,passwd,dbname,db);

    //todo query formating
    String testSql = "SELECT * FROM (SELECT A1.*,ROWNUM RN FROM (SELECT T1.*, T4.AREA_NAME, T2.USER_NAME CONTACT_STAFF_NAME, T5.USER_NAME CLOSE_PERSON_NAME, T3.CODE_NAME EXEC_STATE_DESC, T1.CLOSE_DATE CLOSE_DATE_DESC, CASE WHEN T1.HANG_UP_SMS IS NOT NULL THEN '1' ELSE '0' END HANG_UP_SMS_STATE, T6.MKT_TIGGER_TYPE FROM view1 T1 LEFT JOIN view4 T4 ON T1.AREA_NO = T4.AREA_ID_JT LEFT JOIN view3 T2 ON T1.CONTACT_STAFF = T2.USER_ID LEFT JOIN view3 T5 ON T1.CLOSE_PERSON = T5.USER_ID LEFT JOIN view2 T3 ON T1.EXEC_STATE = T3.CODE_ID AND T1.STATUS_CODE = T3.PREV_CODE_ID AND T3.CODE_TYPE='CONTRACT_FEEDBACK' LEFT JOIN view5 T6 ON T1.MKT_CAMPAIGN_ID = T6.MKT_CAMPAIGN_ID WHERE 1=1 AND T1.CLOSE_PERSON = 12 AND T1.CLOSE_DATE >= '2021-01-18' AND T1.CLOSE_DATE <= '2022-02-18' AND T1.EXEC_STATE IN (1001,7000,1000,5000) ORDER BY T1.hang_up_sms DESC )AS A1 WHERE ROWNUM <= 30000 ) AS A2 WHERE RN > 1;";

    // test3
    testSql = "SELECT * FROM (SELECT A1.*,ROWNUM RN FROM (SELECT T1.*, T4.AREA_NAME, T2.USER_NAME CONTACT_STAFF_NAME, T5.USER_NAME CLOSE_PERSON_NAME, T3.CODE_NAME EXEC_STATE_DESC, T1.CLOSE_DATE CLOSE_DATE_DESC, CASE WHEN T1.HANG_UP_SMS IS NOT NULL THEN '1' ELSE '0' END HANG_UP_SMS_STATE, T6.MKT_TIGGER_TYPE FROM view1 T1 LEFT JOIN view4 T4 ON T1.AREA_NO = T4.AREA_ID_JT LEFT JOIN view3 T2 ON T1.CONTACT_STAFF = T2.USER_ID LEFT JOIN view3 T5 ON T1.CLOSE_PERSON = T5.USER_ID LEFT JOIN view2 T3 ON T1.EXEC_STATE = T3.CODE_ID AND T1.STATUS_CODE = T3.PREV_CODE_ID AND T3.CODE_TYPE='CONTRACT_FEEDBACK' LEFT JOIN view5 T6 ON T1.MKT_CAMPAIGN_ID = T6.MKT_CAMPAIGN_ID WHERE 1=1 AND T1.CLOSE_PERSON = 12 AND T1.CLOSE_DATE >= '2021-01-18' AND T1.CLOSE_DATE <= '2022-02-18' AND T1.EXEC_STATE IN (1001,7000,1000,5000))AS A1 WHERE ROWNUM <= 30000 ) AS A2 WHERE RN > 1 ORDER BY A2.CLOSE_DATE DESC;";
    // test4
    testSql = "select * from (select row_.*, aol_rownum as aol_rownum_ from (select distinct (aol.ol_id) olId, aol.ol_nbr olNbr, aol.so_date soDate, aol.rownum as aol_rownum, (select c.region_name from tab1 c where c.common_region_id = aol.order_region_id) regionName, (select c.region_name from tab1 c where c.common_region_id = aol.so_lan_id) areaName, (select cc.name from tab2 cc where cc.channel_id = aol.channel_id and rownum < 2) channelName, (select '|' || sn1.name from tab3 as sn1 where sn1.staff_id = aol.staff_id and rownum < 2) staffName, (select t.service_name from tab4 t where t.service_kind = aol.service_type) serviceName, (select so.remark from tab5 so where so.service_offer_id = aol.action_type_name) remark, aol.access_number accessNumber from tab6 aol where 1 = 1 and aol.order_region_id < 10000 and aol.so_date >= '2022-01-01 00:00:00' and aol.so_date <= '2022-01-04 00:00:00' and not exists (select 1 from ol_rule_list orl where orl.ol_id = aol.ol_id)) row_ where aol_rownum <= 40000) as table_alias where table_alias.aol_rownum_ >= 0;";

//    testSql = "select * from customer order by c_phone desc;";
//
//    testSql = "SELECT T1.*, T4.AREA_NAME,T2.USER_NAME CONTACT_STAFF_NAME,T5.USER_NAME CLOSE_PERSON_NAME, T3.CODE_NAME EXEC_STATE_DESC, T1.CLOSE_DATE CLOSE_DATE_DESC, CASE WHEN T1.HANG_UP_SMS IS NOT NULL THEN '1' ELSE '0' END HANG_UP_SMS_STATE, T6.MKT_TIGGER_TYPE FROM view1 T1 LEFT JOIN view4 T4 ON T1.AREA_NO = T4.AREA_ID_JT LEFT JOIN view3 T2 ON T1.CONTACT_STAFF = T2.USER_ID LEFT JOIN view3 T5 ON T1.CLOSE_PERSON = T5.USER_ID LEFT JOIN view2 T3 ON T1.EXEC_STATE = T3.CODE_ID AND T1.STATUS_CODE = T3.PREV_CODE_ID AND T3.CODE_TYPE = 'CONTRACT_FEEDBACK' LEFT JOIN view5 T6 ON T1.MKT_CAMPAIGN_ID = T6.MKT_CAMPAIGN_ID WHERE 1 = 1 AND T1.CLOSE_PERSON = 12 AND T1.CLOSE_DATE >= '2021-01-18' AND T1.CLOSE_DATE <= '2022-02-18' AND T1.EXEC_STATE IN (1001, 7000, 1000, 5000) ORDER BY T1.exec_state DESC";
//
//    testSql = "SELECT A1.*, ROWNUM RN FROM ( SELECT T1.*, T4.AREA_NAME,T2.USER_NAME CONTACT_STAFF_NAME,T5.USER_NAME CLOSE_PERSON_NAME, T3.CODE_NAME EXEC_STATE_DESC, T1.CLOSE_DATE CLOSE_DATE_DESC, CASE WHEN T1.HANG_UP_SMS IS NOT NULL THEN '1' ELSE '0' END HANG_UP_SMS_STATE, T6.MKT_TIGGER_TYPE FROM view1 T1 LEFT JOIN view4 T4 ON T1.AREA_NO = T4.AREA_ID_JT LEFT JOIN view3 T2 ON T1.CONTACT_STAFF = T2.USER_ID LEFT JOIN view3 T5 ON T1.CLOSE_PERSON = T5.USER_ID LEFT JOIN view2 T3 ON T1.EXEC_STATE = T3.CODE_ID AND T1.STATUS_CODE = T3.PREV_CODE_ID AND T3.CODE_TYPE = 'CONTRACT_FEEDBACK' LEFT JOIN view5 T6 ON T1.MKT_CAMPAIGN_ID = T6.MKT_CAMPAIGN_ID WHERE 1 = 1 AND T1.CLOSE_PERSON = 12 AND T1.CLOSE_DATE >= '2021-01-18' AND T1.CLOSE_DATE <= '2022-02-18' AND T1.EXEC_STATE IN (1001, 7000, 1000, 5000) ORDER BY T1.exec_state DESC ) AS A1 WHERE ROWNUM <= 30000";
//
//    testSql = "select * from ( select * from customer where c_custkey > 100) as c_all order by c_phone;";

    float origin_cost = db.getCost(testSql);
    System.out.println("origin_cost: " + origin_cost);

    RelNode testRelNode = rewriter.SQL2RA(new Utils().sqlFormattingToCalcite(testSql, rewriter.db.dbname, rewriter.tableList));
    Node resultNode = new Node(testSql,testRelNode,db,origin_cost,rewriter, (float) 0.1,null);
    Node res = resultNode.UTCSEARCH(4, resultNode,1);
    System.out.println("root:"+res.state);
    System.out.println("Original cost: "+origin_cost);
    System.out.println("Optimized cost: "+db.getCost(res.state));
  }



}
