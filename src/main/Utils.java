package main;
import com.alibaba.fastjson.JSONArray;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.CoreRules;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;


import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Utils {
    public static RelOptRule rule2RuleClass(String rule) {
        Map<String, RelOptRule> ruleMap = new HashMap<>();
        // 添加你需要支持的规则和对应的类到 map 中
        ruleMap.put("AGGREGATE_ANY_PULL_UP_CONSTANTS", CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
        ruleMap.put("AGGREGATE_STAR_TABLE", CoreRules.AGGREGATE_STAR_TABLE);
        ruleMap.put("AGGREGATE_PROJECT_STAR_TABLE", CoreRules.AGGREGATE_PROJECT_STAR_TABLE);
        ruleMap.put("AGGREGATE_REDUCE_FUNCTIONS", CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
        ruleMap.put("AGGREGATE_MERGE", CoreRules.AGGREGATE_MERGE);
        ruleMap.put("AGGREGATE_REMOVE", CoreRules.AGGREGATE_REMOVE);
        ruleMap.put("AGGREGATE_EXPAND_DISTINCT_AGGREGATES", CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
        ruleMap.put("AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN", CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN);
        ruleMap.put("AGGREGATE_FILTER_TRANSPOSE", CoreRules.AGGREGATE_FILTER_TRANSPOSE);
        ruleMap.put("AGGREGATE_JOIN_JOIN_REMOVE", CoreRules.AGGREGATE_JOIN_JOIN_REMOVE);
        ruleMap.put("AGGREGATE_JOIN_TRANSPOSE_EXTENDED", CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);
        ruleMap.put("AGGREGATE_UNION_TRANSPOSE", CoreRules.AGGREGATE_UNION_TRANSPOSE);
        ruleMap.put("AGGREGATE_UNION_AGGREGATE", CoreRules.AGGREGATE_UNION_AGGREGATE);
        ruleMap.put("AGGREGATE_UNION_AGGREGATE_FIRST", CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST);
        ruleMap.put("AGGREGATE_UNION_AGGREGATE_SECOND", CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND);
        ruleMap.put("AGGREGATE_CASE_TO_FILTER", CoreRules.AGGREGATE_CASE_TO_FILTER);
        ruleMap.put("CALC_MERGE", CoreRules.CALC_MERGE);
        ruleMap.put("CALC_REMOVE", CoreRules.CALC_REMOVE);
        ruleMap.put("CALC_REDUCE_DECIMALS", CoreRules.CALC_REDUCE_DECIMALS);
        ruleMap.put("CALC_REDUCE_EXPRESSIONS", CoreRules.CALC_REDUCE_EXPRESSIONS);
        ruleMap.put("CALC_SPLIT", CoreRules.CALC_SPLIT);
        ruleMap.put("CALC_TO_WINDOW", CoreRules.CALC_TO_WINDOW);
        ruleMap.put("FILTER_INTO_JOIN", CoreRules.FILTER_INTO_JOIN);
        ruleMap.put("FILTER_MERGE", CoreRules.FILTER_MERGE);
        ruleMap.put("FILTER_CALC_MERGE", CoreRules.FILTER_CALC_MERGE);
        ruleMap.put("FILTER_AGGREGATE_TRANSPOSE", CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        ruleMap.put("FILTER_PROJECT_TRANSPOSE", CoreRules.FILTER_PROJECT_TRANSPOSE);
        ruleMap.put("FILTER_TABLE_FUNCTION_TRANSPOSE", CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE);
        ruleMap.put("FILTER_SCAN", CoreRules.FILTER_SCAN);
        ruleMap.put("FILTER_INTERPRETER_SCAN", CoreRules.FILTER_INTERPRETER_SCAN);
        ruleMap.put("FILTER_CORRELATE", CoreRules.FILTER_CORRELATE);
        ruleMap.put("FILTER_EXPAND_IS_NOT_DISTINCT_FROM", CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM);
        ruleMap.put("FILTER_SET_OP_TRANSPOSE", CoreRules.FILTER_SET_OP_TRANSPOSE);
        ruleMap.put("FILTER_REDUCE_EXPRESSIONS", CoreRules.FILTER_REDUCE_EXPRESSIONS);
        ruleMap.put("INTERSECT_MERGE", CoreRules.INTERSECT_MERGE);
        ruleMap.put("INTERSECT_TO_DISTINCT", CoreRules.INTERSECT_TO_DISTINCT);
        ruleMap.put("MINUS_MERGE", CoreRules.MINUS_MERGE);
        ruleMap.put("PROJECT_AGGREGATE_MERGE", CoreRules.PROJECT_AGGREGATE_MERGE);
        ruleMap.put("PROJECT_CALC_MERGE", CoreRules.PROJECT_CALC_MERGE);
        ruleMap.put("PROJECT_CORRELATE_TRANSPOSE", CoreRules.PROJECT_CORRELATE_TRANSPOSE);
        ruleMap.put("PROJECT_REDUCE_EXPRESSIONS", CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        ruleMap.put("PROJECT_SUB_QUERY_TO_CORRELATE", CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
        ruleMap.put("FILTER_SUB_QUERY_TO_CORRELATE", CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
        ruleMap.put("JOIN_SUB_QUERY_TO_CORRELATE", CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
        ruleMap.put("PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW", CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
        ruleMap.put("PROJECT_TO_SEMI_JOIN", CoreRules.PROJECT_TO_SEMI_JOIN);
        ruleMap.put("PROJECT_JOIN_JOIN_REMOVE", CoreRules.PROJECT_JOIN_JOIN_REMOVE);
        ruleMap.put("PROJECT_JOIN_REMOVE", CoreRules.PROJECT_JOIN_REMOVE);
        ruleMap.put("PROJECT_MERGE", CoreRules.PROJECT_MERGE);
        ruleMap.put("PROJECT_SET_OP_TRANSPOSE", CoreRules.PROJECT_SET_OP_TRANSPOSE);
        ruleMap.put("PROJECT_TABLE_SCAN", CoreRules.PROJECT_TABLE_SCAN);
        ruleMap.put("PROJECT_INTERPRETER_TABLE_SCAN", CoreRules.PROJECT_INTERPRETER_TABLE_SCAN);
        ruleMap.put("PROJECT_WINDOW_TRANSPOSE", CoreRules.PROJECT_WINDOW_TRANSPOSE);
        ruleMap.put("JOIN_CONDITION_PUSH", CoreRules.JOIN_CONDITION_PUSH);
        ruleMap.put("JOIN_ADD_REDUNDANT_SEMI_JOIN", CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN);
        ruleMap.put("JOIN_EXTRACT_FILTER", CoreRules.JOIN_EXTRACT_FILTER);
        ruleMap.put("JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER", CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER);
        ruleMap.put("JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER", CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER);
        ruleMap.put("JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER", CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER);
        ruleMap.put("JOIN_PUSH_EXPRESSIONS", CoreRules.JOIN_PUSH_EXPRESSIONS);
        ruleMap.put("JOIN_PUSH_TRANSITIVE_PREDICATES", CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES);
        ruleMap.put("JOIN_REDUCE_EXPRESSIONS", CoreRules.JOIN_REDUCE_EXPRESSIONS);
        ruleMap.put("JOIN_TO_SEMI_JOIN", CoreRules.JOIN_TO_SEMI_JOIN);
        ruleMap.put("JOIN_LEFT_UNION_TRANSPOSE", CoreRules.JOIN_LEFT_UNION_TRANSPOSE);
        ruleMap.put("JOIN_RIGHT_UNION_TRANSPOSE", CoreRules.JOIN_RIGHT_UNION_TRANSPOSE);
        ruleMap.put("SEMI_JOIN_FILTER_TRANSPOSE", CoreRules.SEMI_JOIN_FILTER_TRANSPOSE);
        ruleMap.put("SEMI_JOIN_PROJECT_TRANSPOSE", CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE);
        ruleMap.put("SEMI_JOIN_JOIN_TRANSPOSE", CoreRules.SEMI_JOIN_JOIN_TRANSPOSE);
        ruleMap.put("SORT_UNION_TRANSPOSE", CoreRules.SORT_UNION_TRANSPOSE);
        ruleMap.put("SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH", CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH);
        ruleMap.put("SORT_JOIN_COPY", CoreRules.SORT_JOIN_COPY);
        ruleMap.put("SORT_REMOVE", CoreRules.SORT_REMOVE);
        ruleMap.put("SORT_REMOVE_CONSTANT_KEYS", CoreRules.SORT_REMOVE_CONSTANT_KEYS);
        ruleMap.put("SORT_JOIN_TRANSPOSE", CoreRules.SORT_JOIN_TRANSPOSE);
        ruleMap.put("SORT_PROJECT_TRANSPOSE", CoreRules.SORT_PROJECT_TRANSPOSE);
        ruleMap.put("UNION_MERGE", CoreRules.UNION_MERGE);
        ruleMap.put("UNION_REMOVE", CoreRules.UNION_REMOVE);
        ruleMap.put("UNION_PULL_UP_CONSTANTS", CoreRules.UNION_PULL_UP_CONSTANTS);
        ruleMap.put("UNION_TO_DISTINCT", CoreRules.UNION_TO_DISTINCT);
        ruleMap.put("AGGREGATE_VALUES", CoreRules.AGGREGATE_VALUES);
        ruleMap.put("FILTER_VALUES_MERGE", CoreRules.FILTER_VALUES_MERGE);
        ruleMap.put("PROJECT_VALUES_MERGE", CoreRules.PROJECT_VALUES_MERGE);
        ruleMap.put("PROJECT_FILTER_VALUES_MERGE", CoreRules.PROJECT_FILTER_VALUES_MERGE);
        ruleMap.put("WINDOW_REDUCE_EXPRESSIONS", CoreRules.WINDOW_REDUCE_EXPRESSIONS);
        ruleMap.put("PROJECT_JOIN_TRANSPOSE", CoreRules.PROJECT_JOIN_TRANSPOSE);
        ruleMap.put("AGGREGATE_PROJECT_MERGE", CoreRules.AGGREGATE_PROJECT_MERGE);
        ruleMap.put("JOIN_TO_CORRELATE", CoreRules.JOIN_TO_CORRELATE);
        ruleMap.put("PROJECT_REMOVE", CoreRules.PROJECT_REMOVE);
        ruleMap.put("FILTER_TO_CALC", CoreRules.FILTER_TO_CALC);
        ruleMap.put("PROJECT_TO_CALC", CoreRules.PROJECT_TO_CALC);
        ruleMap.put("PROJECT_FILTER_TRANSPOSE", CoreRules.PROJECT_FILTER_TRANSPOSE);
        ruleMap.put("PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS", CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS);
        ruleMap.put("PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS", CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS);
        ruleMap.put("DATERANGE_FILTER_INSTANCE", DateRangeRules.FILTER_INSTANCE);
        ruleMap.put("PURNE_UNION_INSTANCE", PruneEmptyRules.UNION_INSTANCE);
        ruleMap.put("PURNE_MINUS_INSTANCE", PruneEmptyRules.MINUS_INSTANCE);
        ruleMap.put("PURNE_INTERSECT_INSTANCE", PruneEmptyRules.INTERSECT_INSTANCE);
        ruleMap.put("PURNE_PROJECT_INSTANCE", PruneEmptyRules.PROJECT_INSTANCE);
        ruleMap.put("PURNE_FILTER_INSTANCE", PruneEmptyRules.FILTER_INSTANCE);
        ruleMap.put("PURNE_SORT_INSTANCE", PruneEmptyRules.SORT_INSTANCE);
        ruleMap.put("PURNE_SORT_FETCH_ZERO_INSTANCE", PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);
        ruleMap.put("PURNE_AGGREGATE_INSTANCE", PruneEmptyRules.AGGREGATE_INSTANCE);
        ruleMap.put("PURNE_JOIN_LEFT_INSTANCE", PruneEmptyRules.JOIN_LEFT_INSTANCE);
        ruleMap.put("PURNE_JOIN_RIGHT_INSTANCE", PruneEmptyRules.JOIN_RIGHT_INSTANCE);
        return ruleMap.get(rule);
    }

    public static void writeContentStringToLocalFile(String str,String path){
        try {
            File file = new File(path);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            file.createNewFile();
            if(str != null && !"".equals(str)){
                FileWriter fw = new FileWriter(file, true);
                fw.write(str);
                fw.flush();
                fw.close();
                System.out.println("执行完毕!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String readStringFromFile(String filePath) {
        String contentStr = "";
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
            contentStr = sb.toString();
            return contentStr;
        } catch (IOException e) {
            e.printStackTrace();
            return "[]";
        }
    }

    public static String[] readWorkloadFromFile(String filePath) {
        String contentStr = Utils.readStringFromFile(filePath);
        String[] result = contentStr.split(";");
        return result;
    }

    public static JSONArray readJsonFile(String filePath) {
        String contentStr = Utils.readStringFromFile(filePath);
        return JSON.parseArray(contentStr);
    }
    public static JSONObject generate_json(Node node) throws Exception {
        JSONObject res = new JSONObject();
        res.put("name",node.name);
        res.put("plan", RelOptUtil.toString(node.state_rel));
        res.put("cost", node.rewriter.db.getCost(node.state));
        Map tmp = new HashMap();
        for(Object k : node.activatedRules.keySet()){
            tmp.put(((RelOptRule) k).toString(),node.activatedRules.get(k));
        }
        int tsz = 0;
        for(Object k :node.rewrite_sequence){
            res.put(String.format("act_rule_%d",++tsz),k);
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

    public static JSONObject getPredicate(RexCall predicates) {
        Collection<SqlKind> collection = new ArrayList();
        collection.add(SqlKind.PLUS);
        collection.add(SqlKind.MINUS);
        collection.add(SqlKind.TIMES);
        collection.add(SqlKind.DIVIDE);

        System.out.println("predicates:" + predicates);
        RexNode columnRexNode = null;
        RexNode valueRexNode = null;
        if(!(predicates.getOperands().get(1) instanceof RexInputRef)){
            columnRexNode = predicates.getOperands().get(0);
            valueRexNode = predicates.getOperands().get(1);
        }else if(!(predicates.getOperands().get(0) instanceof RexInputRef))  {
            columnRexNode = predicates.getOperands().get(1);
            valueRexNode = predicates.getOperands().get(0);
        }else {
            System.out.println("意外之喜predicates:" + predicates);
            return null;
        }
        System.out.println("columnRexNode:" + columnRexNode);
        System.out.println("valueRexNode:" + valueRexNode);

        if (columnRexNode.isA(collection)) {
            // 过滤列的加减乘除
            System.out.println("列的加减乘除不能处理");
            return null;
        }
        String reg = "\\$\\d+";
        Pattern p = Pattern.compile(reg);
        Matcher m = p.matcher(columnRexNode.toString());
        int columnIndex = 0;
        if(m.find()){
            columnIndex = Integer.parseInt(m.group().replace("$", ""));
        }else {
            // 未发现列的索引
            return null;
        }
        String valueString = valueRexNode.toString();
        //  数值的加减乘除计算
        if (valueRexNode.isA(collection)) {
            valueString = Utils.calculateRexNodeString(valueString);
        }else {
            // 去掉DECIMAL标识
            valueString = Utils.formatRexNodeString(valueString);
        }
        if (valueString.contains("$")) {
            System.out.println("Value值中有列:" + valueString);
            return null;
        }
        String oper = predicates.getOperator().toString();
        JSONObject predicate_json = new JSONObject();
        predicate_json.put("column_index", columnIndex);
        predicate_json.put("const",valueString);
        predicate_json.put("operator",oper);
        System.out.println("Success:" + predicate_json);
        return predicate_json;
    }

    public static JSONArray process_clause(RexCall predicates, String logic_oper1, RelNode node){
        Utils.getPredicate(predicates);
        try {
            String oper = predicates.getOperator().toString();
            if (oper == "AND" || oper == "OR"){
                JSONArray resList = new JSONArray();
                if (predicates.getOperands().get(0) instanceof RexCall) {
                    RexCall subpred1 = (RexCall) predicates.getOperands().get(0);
                    JSONArray res_l = process_clause(subpred1,logic_oper1,node);
                    resList.addAll(res_l);
                    for (int i = 1; i < predicates.getOperands().size(); i++) {
                        String logic_oper = oper;
                        RexCall subpred = (RexCall) predicates.getOperands().get(i);
                        JSONArray res = process_clause(subpred,logic_oper,node);
                        resList.addAll(res);
                    }
                }
//                System.out.println("process_clause if 结束:" + resList.toJSONString());
                return resList;
            }else {
                Collection<SqlKind> collection = new ArrayList();
                collection.add(SqlKind.PLUS);
                collection.add(SqlKind.MINUS);
                collection.add(SqlKind.TIMES);
                collection.add(SqlKind.DIVIDE);
//                System.out.println("process_clause else 开始:" + predicates);
                JSONArray resList = new JSONArray();
                if (predicates.getOperands().size() == 0) {
//                    System.out.println("process_clause else 结束:" + resList.toJSONString());
                    return resList;
                }else if (predicates.getOperands().size() == 1) {
                    if(!(predicates.getOperands().get(0) instanceof RexInputRef)){
                        String logic_oper = oper;
                        RexCall subpred = (RexCall) predicates.getOperands().get(0);
                        JSONArray res = process_clause(subpred,logic_oper,node);
                        resList.addAll(res);
//                        System.out.println("process_clause else 结束:" + resList.toJSONString());
                        return resList;
                    }
                } else {
                    if(!(predicates.getOperands().get(1) instanceof RexInputRef)){
                        String  operand1 =  predicates.getOperands().get(0).toString();
                        // 过滤列的加减乘除
                        if (predicates.getOperands().get(0).isA(collection)) {
                            return resList;
                        }
                        String reg = "\\$\\d+";
                        Pattern p = Pattern.compile(reg);
                        Matcher m = p.matcher(operand1);
                        int column_index = 0;
                        if(m.find()){
                            column_index = Integer.parseInt(m.group().replace("$", ""));
                        }else {
                            return resList;
                        }
                        String tableNcolumn = get_relative_columns_with_type(node.getInputs(),column_index);
                        String column_name = tableNcolumn.split("\\.")[1];
                        String  operand2 =  predicates.getOperands().get(1).toString();

                        //  数值的加减乘除计算
                        if (predicates.getOperands().get(1).isA(collection)) {
                            operand2 = Utils.calculateRexNodeString(operand2);
                        }else {
                            // 去掉DECIMAL标识
                            operand2 = Utils.formatRexNodeString(operand2);
                        }

                        JSONObject predicate_json = new JSONObject();
                        predicate_json.put("column",column_name);
                        predicate_json.put("const",operand2);
                        predicate_json.put("context",logic_oper1);
                        predicate_json.put("operator",oper);
                        resList.add(predicate_json);
                        return resList;
                    }
                    else if(!(predicates.getOperands().get(0) instanceof RexInputRef)){
                        String  operand1 =  predicates.getOperands().get(1).toString();

                        // 过滤列的加减乘除
                        if (predicates.getOperands().get(1).isA(collection)) {
                            return resList;
                        }

                        String reg = "\\$\\d+";
                        Pattern p = Pattern.compile(reg);
                        Matcher m = p.matcher(operand1);
                        int column_index = 0;
                        if(m.find()){
                            column_index = Integer.parseInt(m.group().replace("$", ""));
                        }else {
                            return resList;
                        }
                        String tableNcolumn = get_relative_columns_with_type(node.getInputs(),column_index);
                        String column_name = tableNcolumn.split("\\.")[1];
                        String  operand2 =  predicates.getOperands().get(0).toString();
                        // 数值的加减乘除计算
                        if (predicates.getOperands().get(0).isA(collection)) {
                            operand2 = Utils.calculateRexNodeString(operand2);
                        }else {
                            // 去掉DECIMAL标识
                            operand2 = Utils.formatRexNodeString(operand2);
                        }
                        JSONObject predicate_json = new JSONObject();
                        predicate_json.put("column",column_name);
                        predicate_json.put("const",operand2);
                        predicate_json.put("context",logic_oper1);
                        predicate_json.put("operator",oper);
                        resList.add(predicate_json);
                        return resList;
                    }
                }
            }
        } catch (Exception error) {
            error.printStackTrace();
            System.out.println("error:" + error);
        }
        return new JSONArray();
    }

    public static String formatRexNodeString(String rexNodeValue) {
        rexNodeValue = rexNodeValue.replaceAll(" ", "");
        String removeDecimalReg = "[:DECIMAL]+?[/(]\\d+,\\d+[/)]";
        Pattern removeDecimalPattern = Pattern.compile(removeDecimalReg);
        Matcher removeDecimalMatcher = removeDecimalPattern.matcher(rexNodeValue);
        rexNodeValue =removeDecimalMatcher.replaceAll("");
        return rexNodeValue;
    }

    public static String calculateRexNodeString(String rexNodeValue) {
        System.out.println("calculateRexNodeString:"+rexNodeValue);
        try {

            rexNodeValue = Utils.formatRexNodeString(rexNodeValue);

            String reg1 = "[\\*|\\+|\\-|\\/]\\([\\-|\\+]?\\d+(\\.\\d+)*,[\\-|\\+]?\\d+(\\.\\d+)*?\\)";
            Pattern p1 = Pattern.compile(reg1);
            Matcher m1 = p1.matcher(rexNodeValue);
            if(m1.find()){
                String group = m1.group();
                String reg2 = "(\\-|\\+)?\\d+(\\.\\d+)?";
                Pattern p2 = Pattern.compile(reg2);
                Matcher m2 = p2.matcher(group);
                ArrayList resultList = new ArrayList<>();
                if (m2.groupCount() == 2) {
                    while(m2.find()){
                        resultList.add(m2.group(0));
                    }
                }
                String operator = group.substring(0, 1);
                double operatorResult = 0;
                if (operator.equalsIgnoreCase("+")) {
                    operatorResult = Double.valueOf((String)resultList.get(0)) + Double.valueOf((String)resultList.get(1));
                }else if (operator.equalsIgnoreCase("-")) {
                    operatorResult = Double.valueOf((String)resultList.get(0)) - Double.valueOf((String)resultList.get(1));
                }else if (operator.equalsIgnoreCase("*")) {
                    operatorResult = Double.valueOf((String)resultList.get(0)) * Double.valueOf((String)resultList.get(1));
                }else if (operator.equalsIgnoreCase("/")) {
                    operatorResult = Double.valueOf((String)resultList.get(0)) / Double.valueOf((String)resultList.get(1));
                }
                rexNodeValue = m1.replaceFirst(String.valueOf(operatorResult));
                return Utils.calculateRexNodeString(rexNodeValue);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return rexNodeValue;
    }

    public static JSONObject getConditionFromRelNode(RelNode rel_node) {

        JSONObject parse_res = new JSONObject();
        try {
            Deque stack = new LinkedList();
            stack.add(rel_node);
            JSONArray res = new JSONArray();
            while (stack.size()>0) {
                RelNode node = (RelNode) stack.pop();
                for (int i = 0; i < node.getInputs().size(); i++) {
                    stack.add(node.getInputs().get(i));
                }
                JSONArray clause_res;
                RexNode condition = null;
                if (node instanceof LogicalFilter) {
                    condition = ((LogicalFilter) node).getCondition();
                } else if (node instanceof LogicalJoin) {
                    condition = ((LogicalJoin) node).getCondition();
                }
                if (condition instanceof RexCall) {
                    clause_res = process_clause((RexCall) condition, "null" ,node);
                    res.addAll(clause_res);
                }
            }
            parse_res.put("conditions",res);
        } catch (Exception e) {
            e.printStackTrace();
            parse_res.put("conditions", new ArrayList());
        }
        return parse_res;
    }

    static String get_relative_columns_with_type (List<RelNode> childs, int index){
        String res = "";
        if (index<childs.get(0).getRowType().getFieldNames().size()){
            if(childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index) != null){
                res += childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index).getOriginTable().getQualifiedName().get(0);
                res += "."+childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index).getOriginTable().getRowType().getFieldNames().get(childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index).getOriginColumnOrdinal());
                res += "."+childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index).getOriginTable().getRowType().getFieldList().get(childs.get(0).getCluster().getMetadataQuery().getColumnOrigin(childs.get(0),index).getOriginColumnOrdinal()).getType();
            }
        }
        else {
            index-=childs.get(0).getRowType().getFieldNames().size();
            if(childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1), index) != null){
                res += childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1),index).getOriginTable().getQualifiedName().get(0);
                res += "."+childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1),index).getOriginTable().getRowType().getFieldNames().get(childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1),index).getOriginColumnOrdinal());
                res += "."+childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1),index).getOriginTable().getRowType().getFieldList().get(childs.get(1).getCluster().getMetadataQuery().getColumnOrigin(childs.get(1),index).getOriginColumnOrdinal()).getType();
            }
        }
        return res;
    }
    public static void dfs_mtcs_tree(Node node, int depth){
        if(node.parent == null){
            System.out.println("Original Query");
            System.out.println(node.state_rel.explain());
            // System.out.println(node.activatedRules);
            return;
        }
        dfs_mtcs_tree(node.parent, depth + 1);
        System.out.println(node.activatedRules);
        System.out.println(node.state_rel.explain());
    }
}
