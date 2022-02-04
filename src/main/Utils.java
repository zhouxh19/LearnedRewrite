package main;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import java.util.ArrayList;

class FormatColumn {

    class ReplaceColumnAndLongValues extends ExpressionDeParser implements main.ReplaceColumnAndLongValues {
        ArrayList tables;
        String sql;

        public ReplaceColumnAndLongValues(String sql, ArrayList tables) {
            this.tables = tables;
            this.sql = sql;
        }
        @Override
        public void visit(Column column) {

            String columnName = column.getColumnName();
            if (!this.sql.contains("as " + columnName)) {
                if (!(columnName.equalsIgnoreCase("false") || columnName.equalsIgnoreCase("true"))) {
                    columnName = "\"" + columnName + "\"";
                }
            }

            if (column.getTable() != null) {
                if (this.tables.contains(column.getTable().toString())) {
                    this.getBuffer().append("\"" + column.getTable().toString() + "\"" + "." + columnName);
                }else {
                    this.getBuffer().append(column.getTable().toString() + "." + columnName);
                }
            }else {
                this.getBuffer().append(columnName);
            }
        }
    }

    public String format(String sql, ArrayList tables) throws JSQLParserException {
        StringBuilder buffer = new StringBuilder();
        ExpressionDeParser expr = new ReplaceColumnAndLongValues(sql, tables);
        SelectDeParser selectDeparser = new SelectDeParser(expr, buffer);
        expr.setSelectVisitor(selectDeparser);
        expr.setBuffer(buffer);
        StatementDeParser stmtDeparser = new StatementDeParser(expr, selectDeparser, buffer);
        Statement stmt = CCJSqlParserUtil.parse(sql);
        stmt.accept(stmtDeparser);
        return stmtDeparser.getBuffer().toString();
    }
}

class TableAddSchema {
    public static String addSchema(String sql, String dbname) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(sql);
        if (!(statement instanceof Select)) {
            return sql;
        }
        Select select = null;
        try {
            select = (Select)CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            System.out.println(e);
        }
        //Replace Table1 with hive.DB1.Table1 and Table2 with mongo.DB2.Table2
        ExpressionDeParser expressionDeParser = new ExpressionDeParser();
        StringBuilder buffer = new StringBuilder();
        SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer) {
            @Override
            public void visit(Table tableName) {
                String schemaName = dbname;
                if (tableName.getAlias() != null) {
                    this.getBuffer().append("\"" + schemaName + "\"." + "\"" + tableName.getName() + "\"" + tableName.getAlias().toString());
                }else {
                    this.getBuffer().append("\"" + schemaName + "\"." + "\"" + tableName.getName() + "\"");
                }
            }
        };
        expressionDeParser.setSelectVisitor(deparser);
        expressionDeParser.setBuffer(buffer);
        select.getSelectBody().accept(deparser);
        return buffer.toString();
    }
}

public class Utils {
    public String sqlFormattingToCalcite(String sql, String dbname, ArrayList tableList){
        // 先反向格式化一下，防止二次格式化
        sql = this.sqlFormattingFromCalcite(sql, dbname);
        FormatColumn formatColumn = new FormatColumn();
        try {
            sql = formatColumn.format(sql, tableList);
        } catch (JSQLParserException e) {
            System.out.println("formatColumn Error: "+ e);
        }
        TableAddSchema tableAddSchema = new TableAddSchema();
        try {
            sql = tableAddSchema.addSchema(sql, dbname);
        } catch (JSQLParserException e) {
            System.out.println("TableAddSchema Error: "+ e);
        }
        System.out.println("sqlFormattingToCalcite: "+ sql);
        return sql;
    }

    public String sqlFormattingFromCalcite(String sql, String dbname){
        sql = sql.replace("\"", "");
        sql = sql.replace(dbname + '.', "");
        sql = sql.replace("$", "");
        sql = sql.replace("SINGLE_VALUE", "");
        return sql;
    }

}
