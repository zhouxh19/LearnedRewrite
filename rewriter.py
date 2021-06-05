
import os
import sqlparse
import time

from mcts import *
from draw import drawPolicyTree
from database import RewriteState

import jpype as jp
from jpype.types import *
import jpype.imports

from draw import drawPolicyTree

''' Configure JAVA environment for JPype '''
base_dir = os.path.abspath(os.curdir)
local_lib_dir = os.path.join(base_dir, 'libs')

# For the first use: uncomment if `classpath.txt` need update
# Otherwise: commoent "_ = os.popen('mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt').read()"
_ = os.popen('mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt').read()

classpath = open(os.path.join(base_dir, 'classpath.txt'), 'r').readline().split(':')
classpath.extend([os.path.join(local_lib_dir, jar) for jar in os.listdir(local_lib_dir)])
# print('\n'.join(classpath))

if not jp.isJVMStarted():
    jp.startJVM(jp.getDefaultJVMPath(), classpath=classpath)

from javax.sql import DataSource
from java.sql import Connection, DriverManager
from java.util import ArrayList, List

from org.postgresql import Driver as PostgreSQLDriver

import org.apache.calcite.rel.rules as R

from org.apache.calcite.adapter.jdbc import JdbcSchema
from org.apache.calcite.jdbc         import CalciteConnection
from org.apache.calcite.plan         import RelOptUtil, RelOptRule
from org.apache.calcite.plan.hep     import HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder
from org.apache.calcite.rel          import RelRoot, RelNode
from org.apache.calcite.rel.rel2sql  import RelToSqlConverter

from org.apache.calcite.rel.rules    import FilterJoinRule,AggregateExtractProjectRule,FilterMergeRule

from org.apache.calcite.schema       import SchemaPlus
from org.apache.calcite.sql          import SqlNode, SqlDialect
from org.apache.calcite.sql.dialect  import CalciteSqlDialect, PostgresqlSqlDialect
from org.apache.calcite.tools        import FrameworkConfig, Frameworks, Planner, RelBuilderFactory
from org.apache.calcite.util         import SourceStringReader



class Rewriter():

    def __init__(self, args, db):

        try:
            if planner: pass
        except:
            conn = DriverManager.getConnection('jdbc:calcite:')
            calcite_conn = conn.unwrap(CalciteConnection)
            root_schema = calcite_conn.getRootSchema()
            # database config
            data_source = JdbcSchema.dataSource("jdbc:postgresql://"+args.host+':'+str(args.port)+'/',
                                                args.driver, args.user, args.password)
            schema = root_schema.add(args.dbname, JdbcSchema.create(root_schema, args.dbname, data_source, None, None))
            config = Frameworks.newConfigBuilder().defaultSchema(schema).build()
            planner = Frameworks.getPlanner(config)
        print('planner configured')

        try:
            if dialect: pass
        except:
            dialect = PostgresqlSqlDialect.DEFAULT
        print('dialect configured')

        # rule list
        ruledir = jp.JPackage('org.apache.calcite.rel.rules')
        self.rulelist = ["ruledir.PruneEmptyRules.FILTER_INSTANCE", "ruledir.PruneEmptyRules.PROJECT_INSTANCE",
                    "ruledir.PruneEmptyRules.SORT_INSTANCE",
                    "ruledir.PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE", "ruledir.PruneEmptyRules.AGGREGATE_INSTANCE",
                    "ruledir.PruneEmptyRules.JOIN_LEFT_INSTANCE",
                    "ruledir.PruneEmptyRules.JOIN_RIGHT_INSTANCE"]

        self.planner = planner
        self.dialect = dialect

        self.db = db

    def parse_quote(self, sql):
        new_sql = ""
        #print(sql)
        sql = str(sql)
        for token in sqlparse.parse(sql)[0].flatten():
            if token.ttype is sqlparse.tokens.Name and token.parent and not isinstance(token.parent.parent,
                                                                                       sqlparse.sql.Function):
                new_sql += '\"' + token.value + '\"'
            elif token.value != ';':
                new_sql += token.value

        return new_sql

    def SQL2RA(self, sql, ruleid):
        self.planner.close()
        self.planner.reset()
        sql_node = self.planner.parse(SourceStringReader(sql))
        sql_node = self.planner.validate(sql_node)
        rel_root = self.planner.rel(sql_node)
        rel_node = rel_root.project()
        ruledir = jp.JPackage('org.apache.calcite.rel.rules')
        # print("test rule:" + self.rulelist[ruleid])
        rule = eval(self.rulelist[ruleid])

        if ruleid == -1:
            program = HepProgramBuilder().addMatchOrder(HepMatchOrder.TOP_DOWN).build()

            return 1, rel_node
        else:
            # judge whether the rule can be applied to the query (rel_node)
            if rule.getOperand().matches(rel_node) == True:
                program = HepProgramBuilder().addMatchOrder(HepMatchOrder.TOP_DOWN).build()
                hep_planner = HepPlanner(program)

                # hep_planner.addRule(PruneEmptyRules.PROJECT_INSTANCE)
                hep_planner.clear()
                hep_planner.addRule(rule)  # how to select one

                hep_planner.setRoot(rel_node)
                rel_node = hep_planner.findBestExp()

                return 1, rel_node
            else:
                return 0, rel_node

    def RA2SQL(self, ra):
        converter = RelToSqlConverter(self.dialect)

        return converter.visitInput(ra, 0).asStatement().toSqlString(self.dialect).getSql()

    def single_rewrite(self, sql, ruleid):

        # sql = sql.replace(";", "")
        # sql = sql.lower()

        # print(' formatted sql '.center(60, '-'))
        format_sql = self.parse_quote(sql)
        # print(format_sql)

        is_rewritten,ra = self.SQL2RA(format_sql, ruleid)
        if is_rewritten == 1: # the rule can be applied
            # print(' rewritten sql '.center(60, '-'))
            sql = self.RA2SQL(ra)

            success,res = self.db.execute_sql(str(sql))
            if success == 1:
                return 1,str(sql)
            else:
                return 0,''
        else:
            return 0,''
'''
    def single_rewrite_check(self, sql, ruleid):

        sql = sql.replace(";", "")
        sql = sql.lower()

        

        sql_node = self.planner.parse(SourceStringReader(sql))
        print(sql_node)
        sql_node = self.planner.validate(sql_node)
        rel_root = self.planner.rel(sql_node)
        rel_node = rel_root.project()

        ruledir = jp.JPackage('org.apache.calcite.rel.rules')
        # judge whether the rule works
        if eval(self.rulelist[ruleid]).getOperand().matches(rel_node) == True:
            sql = self.single_rewrite(sql, ruleid)
            # judge whether this sql is executable
            if self.db.execute_sql(sql) != '':
                return True

        return False
'''

def rewrite(sql, args, db, origin_cost, file):

    rewriter = Rewriter(args, db)

    if args.rewrite_policy == 'topdown': # -1
        is_rewritten, sql = rewriter.single_rewrite(sql, -1)

    elif args.rewrite_policy == 'mcts':

        current_node = Node(sql, db, origin_cost, rewriter, args.gamma) # root
        best_node = current_node

        for l in range(args.num_turns):
            best_node = UCTSEARCH(args.num_sims / (l + 1), best_node)
            print("level %d" % l)
            #print("Num Children: %d" % len(best_node.children))
            #for i, c in enumerate(best_node.children):
            #    print(i, c)
            print("Best Child: {} {}".format(best_node.state, best_node.reward))

            print("--------------------------------")

            drawPolicyTree(current_node, file.replace('.sql',''))

        sql = best_node.state

    else:
        print("Fault Policy!")
        exit()

    return sql