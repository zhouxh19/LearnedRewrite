package main;
import com.alibaba.fastjson.JSONArray;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

    public static JSONArray readJsonFile(String filePath) {
        String jsonStr = "";
        try {
            File jsonFile = new File(filePath);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile),"utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return JSON.parseArray(jsonStr);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static JSONObject generate_json(Node node) throws Exception {
        JSONObject res = new JSONObject();
        res.put("name",node.name);
        res.put("plan", RelOptUtil.toString(node.state_rel));
        res.put("cost", node.rewriter.getCostRecordFromRelNode(node.state_rel));
        Map tmp = new HashMap();
        for(Object k : node.activatedRules.keySet()){
            tmp.put(((RelOptRule) k).toString(),node.activatedRules.get(k));
        }
        res.put("activated_rules",tmp);

        List children = node.children;
        List<JSONObject> children_jsons = new ArrayList<>();
        for(int i = 0;i<children.size();i++){
            children_jsons.add(generate_json((Node) children.get(i)));
        }
        res.put("children",children_jsons);
        return res;
    }
}
