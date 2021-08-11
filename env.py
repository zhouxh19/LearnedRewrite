import numpy as np
import pymysql
import psycopg2
from treelib import Tree
from enum import IntEnum
import re
import math
import os

np.set_printoptions(threshold=np.inf)

# support grammar key word

operator = ['=', '!=', '>', '<', '<=', '>=']
order_by_key = ['DESC', 'ASC']
# predicate_type = ['between', 'is null', 'is not null', 'in', 'is not in', 'exists', 'not exists', 'like', 'not like']
predicate_type = ['between', 'like', 'not like']
# conjunction = ['and', 'or']
conjunction = ['and']
aggregate = ['count', 'max', 'min', 'avg', 'sum']
# keyword = ['select', 'from', 'aggregate', 'where', 'group by', 'having', 'order by']
keyword = ['select', 'from', 'aggregate', 'where', 'having', 'order by'] # group by是被迫的去掉了
join = ['join', 'cartesian']
# as_name = ['tmp1', 'tmp2', 'tmp3', 'tmp4', 'tmp5']  # as 空间得固定住
# integer = [str(i) for i in range(50)]


class DataNode(object):
    # action_index 与 identifier不同, action_index是map里面的，identifire是tree里面的
    def __init__(self, action_index, datatype=None, key_type=None):
        self.action_index = action_index
        self.datatype = datatype
        self.key_type = key_type


class RelationGraph(object):
    """
    维护表和表外键的关系，这样可以知道哪些表可以连接，有意义的连接
    """
    def __init__(self):
        self.relation_graph = {}

    def add_relation(self, begin, to, relation):
        if begin not in self.relation_graph.keys():
            self.relation_graph[begin] = {}
        self.relation_graph[begin][to] = relation

    def get_relation(self, table):
        return set(self.relation_graph[table].keys())

    def get_relation_key(self, begin, end):
        return self.relation_graph[begin][end]

# class GrammarNode(object):
#     def __init__(self, space = None, observe = None, action = None):
#         self.action = action
#         self.observe = observe
#         self.space = space


class DataType(IntEnum):
    VALUE = 0
    TIME = 1
    CHAR = 2


AGGREGATE_CONSTRAINTS = {
    DataType.VALUE.value: ['count', 'max', 'min', 'avg', 'sum'],
    DataType.VALUE.CHAR: ['count', 'max', 'min'],
    DataType.VALUE.TIME: ['count', 'max', 'min']
}
# 一些设定 #
# 聚合函数的出现一定会导致group by
# group by 一定程度出现having 对输出结果的控制


def transfer_field_type(database_type, server):
    data_type = list()
    if server == 'mysql':
        data_type = [['int', 'tinyint', 'smallint', 'mediumint', 'bigint', 'float', 'double', 'decimal'],
                     ['date', 'time', 'year', 'datetime', 'timestamp']]
        database_type = database_type.lower().split('(')[0]
    elif server == 'postgresql':
        data_type = [['integer', 'numeric'],
                     ['date']]
    if database_type in data_type[0]:
        return DataType.VALUE.value
    elif database_type in data_type[1]:
        return DataType.TIME.value
    else:
        return DataType.CHAR.value


def loadfile(filepath):
    lists = []
    with open(filepath, 'r') as f:
        for line in f.readlines():
            lists.append(line.rstrip('\n'))
        f.close()
        lists = list(set(lists))    # 单个去重
        return lists


def connect_server(dbname, server_name):
    if server_name == 'mysql':
        db = pymysql.connect(host="localhost", user="root", passwd="", db=dbname, charset="utf8")
        cursor = db.cursor()
        return db, cursor
    elif server_name == 'postgresql':
        db = psycopg2.connect(database=dbname, user="lixizhang", password="xi10261026zhang", host="166.111.5.177", port="5433")
        cursor = db.cursor()
        return db, cursor
    else:
        print('数据库连接不上...')
        return


def get_table_structure(cursor, server):
    """
    schema: {table_name: {field_name {'DataType', 'keytype'}}}
    :param cursor:
    :return:
    """
    if server == 'mysql':
        cursor.execute('SHOW TABLES')
        tables = cursor.fetchall()
        schema = {}
        for table_info in tables:
            table_name = table_info[0]
            sql = 'SHOW COLUMNS FROM ' + table_name
            cursor.execute(sql)
            columns = cursor.fetchall()
            schema[table_name] = {}
            for col in columns:
                schema[table_name][col[0]] = [transfer_field_type(col[1], server), col[3]]
            return schema
    elif server == 'postgresql':
        cursor.execute('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';')
        tables = cursor.fetchall()
        schema = {}
        for table_info in tables:
            table_name = table_info[0]
            sql = 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'' + table_name + '\';'
            cursor.execute(sql)
            columns = cursor.fetchall()
            schema[table_name] = {}
            for col in columns:
                schema[table_name][col[0]] = [transfer_field_type(col[1], server)]
        # table_name = tables[0][0]
        # sql = 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'' + table_name + '\';'
        # cursor.execute(sql)
        # columns = cursor.fetchall()
        # schema[table_name] = {}
        # for col in columns:
        #     schema[table_name][col[0]] = [transfer_field_type(col[1], server)]
        return schema


def load_statitics(tables, types, attributes):
    conn = psycopg2.connect(dbname='tpcc', user='123', password='123', host='172.6.31.13', port='5432')
    cur = conn.cursor()
    statistics = {}
    for table in tables:
        cur.execute('select tablename, attname, avg_width, n_distinct, most_common_vals, most_common_freqs, histogram_bounds from pg_stats where tablename = \''+table+'\';')
        rows = cur.fetchall()
        for row in rows:
            attname = str(row[1])
            print (table, attname)
            T = types[table][attributes[table].index(attname)]
            avg_width = float(row[2])
            n_distinct = int(row[3])
            most_common_vals, most_common_freqs, histogram_bounds = None, None, None
            if row[4] is not None:
                most_common_vals = [T(x.strip()) for x in str(row[4]).strip('{}').split(',')]
            if row[5] is not None:
                most_common_freqs = [float(x.strip()) for x in str(row[5]).strip('[]').split(',')]
            if row[6] is not None:
                histogram_bounds = [T(x.strip()) for x in str(row[6]).strip('{}').split(',')]
            statistics[attname] = {'avg_width': avg_width, 'n_distinct': n_distinct,
                                   'most_common_vals': most_common_vals, 'most_common_freqs': most_common_freqs,
                                   'histogram_bounds': histogram_bounds, 'type': T}
    return statistics


def selectivity_estimation(attname, op, value, stat):
    statistic = stat[attname]
    T = statistic['type']
    value = T(value)
    avg_width = statistic['avg_width']
    n_distinct = statistic['n_distinct']
    most_common_vals, most_common_freqs, histogram_bounds = statistic['most_common_vals'], statistic['most_common_freqs'], statistic['histogram_bounds']
    selectivity = 0.0
    if op in ['<', '<=']:
        if histogram_bounds is not None:
            for idx, v in enumerate(histogram_bounds):
                if v >= value:
                    if idx > 0:
                        if value is float or value is int:
                            selectivity += float(value - histogram_bounds[idx-1]) / (v - histogram_bounds[idx-1]) / len(histogram_bounds)
                        selectivity += float(idx-1) / len(histogram_bounds)
                    break
        if most_common_vals is not None:
            if op == '<':
                for idx, val in enumerate(most_common_vals):
                    if val < value:
                        selectivity += most_common_freqs[idx]
            elif op == '<=':
                for idx, val in enumerate(most_common_vals):
                    if val <= value:
                        selectivity += most_common_freqs[idx]
    elif op in ['>', '>=']:
        if histogram_bounds is not None:
            for idx in reversed(range(len(histogram_bounds))):
                v = histogram_bounds[idx]
                if v <= value:
                    if idx < len(histogram_bounds) - 1:
                        if value is float or value is int:
                            selectivity += float(value - v) / (histogram_bounds[idx+1] - v) / len(histogram_bounds)
                        selectivity += float(len(histogram_bounds) - 2 - idx) / len(histogram_bounds)
                    break
        if most_common_vals is not None:
            if op == '>':
                for idx, val in enumerate(most_common_vals):
                    if val > value:
                        selectivity += most_common_freqs[idx]
            elif op == '>=':
                for idx, val in enumerate(most_common_vals):
                    if val >= value:
                        selectivity += most_common_freqs[idx]
    elif op == '=' and most_common_vals is not None and value in most_common_vals:
        selectivity += most_common_freqs[most_common_vals.index(value)]
    if selectivity == 0.0:
        if histogram_bounds is not None:
            selectivity += 1.0 / len(histogram_bounds) / avg_width
    return selectivity


def get_tables_sample_data(dbname, schema):
    sample_data = {}
    root_path = os.path.abspath('..')
    for table_name in schema:
        sample_data[table_name] = {}
        for field in schema[table_name]:
            path = root_path + '/' + dbname + '/' + table_name + '/' + field + '.txt'
            sample_data[table_name][field] = loadfile(path)
    return sample_data


def cal_expect_cost():
    with open('./queries/sql_cost') as f:
        total = 0
        count = 0
        for line in f.readlines():
            line = line.strip(('\n'))
            total += float(line)
            count += 1
    f.close()
    return total / count


class GenSqlEnv(object):
    def __init__(self, metric, dbname, target_type, server_name='postgresql'):

        # self.expect_cost = cal_expect_cost()
        # print("expect sql cost:", self.expect_cost)
        self.target = metric
        self.target_type = target_type  # target_type(0: cost, 1:cardinality)

        self.log_target = math.log(metric, 1.5)
        self.dbname = dbname
        self.server_name = server_name

        self.db, self.cursor = connect_server(dbname, server_name=server_name)

        self.step_reward = 0
        self.bug_reward = 1
        self.terminal_word = " "  # 空格结束、map在0上,index为0

        self.word_num_map, self.num_word_map, self.relation_tree, self.relation_graph = self._build_relation_env(server_name)

        self.action_space = self.observation_space = len(self.word_num_map)

        # self.grammar_tree = self._build_grammar_env()
        self.select_space = []
        self.from_space = []
        self.where_space = []
        self.group_by_space = []
        self.having_space = []
        self.order_by_space = []
        self.aggregate_space = []

        self.group_key = False

        self.operator = [self.word_num_map[x] for x in operator]
        # self.order_by_key = [self.word_num_map[x] for x in order_by_key]
        self.predicate_type = [self.word_num_map[x] for x in predicate_type]
        self.conjunction = [self.word_num_map[x] for x in conjunction]
        # self.aggregate = [self.word_num_map[x] for x in aggregate]
        self.keyword = [self.word_num_map[x] for x in keyword]
        # self.integer = [self.word_num_map[x] for x in integer]
        self.join = [self.word_num_map[x] for x in join]

        self.attributes = []

        table_node = self.relation_tree.children(self.relation_tree.root)
        self.tables = [field.identifier for field in table_node]
        for node in table_node:
             self.attributes += [field.identifier for field in self.relation_tree.children(node.identifier)]

        # print(self.relation_table)
        self.select_clause = self.from_clause = self.where_clause = self.group_by_clause = self.having_clause = self.order_by_clause = self.aggregate_clause = ""

        self.master_control = {
            'select': [self.select_observe, self.select_action],
            'from': [self.from_observe, self.from_action],
            'where': [self.where_observe, self.where_action],
            # 'group by': [self.group_by_observe, self.group_by_action],
            'having': [self.having_observe, self.having_action],
            'order by': [self.order_by_observe, self.order_by_action],
            'aggregate': [self.aggregate_observe, self.aggregate_action],
        }

        self.cur_state = self.master_control['from']  # 初始时为from
        self.time_step = 0

    def _build_relation_env(self, server_name):
        print("_build_env")

        schema = get_table_structure(self.cursor, server_name)
        sample_data = get_tables_sample_data(self.dbname, schema)

        tree = Tree()
        tree.create_node("root", 0, None, data=DataNode(0))

        word_num_map = dict()
        num_word_map = dict()

        word_num_map[self.terminal_word] = 0
        num_word_map[0] = self.terminal_word

        # 第一层 table_names
        count = 1
        for table_name in schema.keys():
            tree.create_node(table_name, count, parent=0, data=DataNode(count, datatype="table_name"))
            word_num_map[table_name] = count
            num_word_map[count] = table_name
            count += 1

        # 第二层 table的attributes
        for table_name in schema.keys():
            for field in schema[table_name].keys():
                attribute = '{0}.{1}'.format(table_name, field)
                tree.create_node(attribute, count, parent=word_num_map[table_name],
                                 data=DataNode(count, datatype=schema[table_name][field][0]))
    #                                           key_type=schema[table_name][field][1])) # postgresql 取 key太麻烦了
                word_num_map[attribute] = count
                num_word_map[count] = attribute
                count += 1

        # 关系图
        relation_graph = RelationGraph()
        for table_name in schema.keys():
            sql = '''
            select                                                                       
                  tc.table_name, kcu.column_name, 
                  ccu.table_name AS foreign_table_name,
                  ccu.column_name AS foreign_column_name
            FROM 
                  information_schema.table_constraints AS tc 
                  JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
                  JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name = '{}';
            '''.format(table_name)
            self.cursor.execute(sql)
            relations = self.cursor.fetchall()
            for relation in relations:
                relation_from = '{0}.{1}'.format(table_name, relation[1])
                relation_to = '{0}.{1}'.format(relation[2], relation[3])
                relation_graph.add_relation(word_num_map[table_name], word_num_map[relation[2]], (relation_from, relation_to))
                relation_graph.add_relation(word_num_map[relation[2]], word_num_map[table_name], (relation_to, relation_from))

        # 第三层 每个taoble的sample data
        for table_name in schema.keys():
            for field in schema[table_name].keys():
                for data in sample_data[table_name][field]:
                    if data in word_num_map.keys():
                        pass
                    else:
                        word_num_map[data] = len(num_word_map)
                        num_word_map[len(num_word_map)] = data
                    field_name = '{0}.{1}'.format(table_name, field)
                    tree.create_node(data, count, parent=word_num_map[field_name], data=DataNode(word_num_map[data]))
                    count += 1

        self.add_map(operator, word_num_map, num_word_map)
        # self.add_map(order_by_key, word_num_map, num_word_map)
        self.add_map(predicate_type, word_num_map, num_word_map)
        self.add_map(conjunction, word_num_map, num_word_map)
        # self.add_map(aggregate, word_num_map, num_word_map)
        self.add_map(keyword, word_num_map, num_word_map)
        # self.add_map(integer, word_num_map,num_word_map)
        self.add_map(join, word_num_map, num_word_map)

        print("_build_env done...")
        print("action/observation space:", len(num_word_map), len(word_num_map))
        print("relation tree size:", tree.size())
        # tree.show()
        return word_num_map, num_word_map, tree, relation_graph

    # def _build_grammar_env(self):
    #     print('build grammar env')
    #     grammar = {'select', 'from', 'where', 'group by', 'having', 'order by'}
    #     tree = Tree()
    #     tree.create_node('root', 'root', None, data=GrammarNode(0))
    #     for word in grammar:
    #         tree.create_node(word, word, parent='root',
    #                          data=GrammarNode(space=np.zeros((self.action_space,), dtype=int),
    #                                           observe=self.master_control[word][0],
    #                                           action=self.master_control[word][1]
    #                                           )
    #                          )
    #     tree.create_node('aggregate', 'aggregate', 'select', data
    #     =GrammarNode(space=np.zeros((self.action_space,), dtype=int),
    #     observe=self.master_control['aggregate'][0],
    #     action=self.master_control['aggregate'][1]))
    #
    #     return tree

    def reset(self):
        # print("reset")
        self.cur_state = self.master_control['from']
        self.select_clause = self.from_clause = self.where_clause = self.group_by_clause = self.having_clause = self.order_by_clause = self.aggregate_clause = ""
        self.where_space.clear()
        self.from_space.clear()
        self.select_space.clear()
        self.aggregate_space.clear()
        self.group_by_space.clear()
        self.order_by_space.clear()
        self.having_space.clear()
        self.time_step = 0
        self.group_key = False
        return self.word_num_map['from']

    def activate_space(self, cur_space, keyword):   # 用keyword开启 cur_space 到 next_space 的门
        # 激活下一个space
        cur_space[keyword] = 1

    def activate_ternminal(self, cur_space):
        cur_space[0] = 1

    def select_observe(self, observation):
        # self.cur_sql[: -1]
        candidate_word = np.zeros((self.action_space,), dtype=int)
        if self.num_word_map[observation] == 'select' or observation in self.join:     # 第一次进
            self.need_select_table = self.from_space.copy()
            for table_index in self.need_select_table:
                candidate_word[[field.identifier for field in self.relation_tree.children(table_index)]] = 1
            return candidate_word
        else:   # attribtue
            if self.need_select_table:
                for table_index in self.need_select_table:
                    candidate_word[[field.identifier for field in self.relation_tree.children(table_index)]] = 1
                return candidate_word
            else:   # table和普通的attribute选完了可以聚合也可以where condition 也可以orderby
                self.activate_space(candidate_word, self.word_num_map['aggregate'])
                self.activate_space(candidate_word, self.word_num_map['where'])
                self.activate_space(candidate_word, self.word_num_map['order by'])
                self.activate_ternminal(candidate_word)
                return candidate_word

    def select_action(self, action):
        # print('enter select_action:', self.num_word_map[action])
        if self.num_word_map[action] == 'select' or action in self.join:
            self.select_clause = 'select'
        elif action in self.keyword:
            self.cur_state = self.master_control[self.num_word_map[action]]
            self.cur_state[1](action)
        else:
            self.select_space.append(action)
            self.group_by_space.append(action)
            self.order_by_space.append(action)
            table_name_index = self.relation_tree.parent(action).identifier   #
            self.need_select_table.remove(table_name_index)
            self.select_clause = self.select_clause + ' ' + self.num_word_map[action] + ','
        return self.step_reward, 0

    def aggregate_observe(self, observation=None):
        candidate_word = np.zeros((self.action_space,), dtype=int)
        if self.group_key is False:
            self.group_by_generate()    # 直接group by产生
            self.group_key = True
        self.activate_space(candidate_word, self.word_num_map['aggregate'])
        self.activate_space(candidate_word, self.word_num_map['where'])
        self.activate_space(candidate_word, self.word_num_map['order by'])
        self.activate_space(candidate_word, self.word_num_map['having'])
        self.activate_ternminal(candidate_word)
        return candidate_word

    def aggregate_action(self, action):
        if action == self.word_num_map['aggregate']:
            while True:
                table = np.random.choice(self.from_space)
                attributes = [node.identifier for node in self.relation_tree.children(table)]
                choose_attribute = np.random.choice(attributes)
                choose_aggregate_type = np.random.choice(AGGREGATE_CONSTRAINTS[self.relation_tree.get_node(choose_attribute).data.datatype])
                if (choose_aggregate_type, choose_attribute) not in self.aggregate_space:
                    break
            self.aggregate_space.append((choose_aggregate_type, choose_attribute))
            self.aggregate_clause = self.aggregate_clause + ' ' + '{aggregate_type}({aggregate_attribute})'.format(
                aggregate_type=choose_aggregate_type, aggregate_attribute=self.num_word_map[choose_attribute]) + ','
        else:   # 其他key_word
            self.cur_state = self.master_control[self.num_word_map[action]]
            self.cur_state[1](action)
        return self.step_reward, 0

    def from_observe(self, observation=None):
        if observation == self.word_num_map['from']:    # 第一次进来
            self.from_clause = 'from'
            candidate_tables = np.zeros((self.action_space,), dtype=int)
            candidate_tables[self.tables] = 1
            return candidate_tables
        else: # observation in self.tables:   # 选择table 激活join type
            relation_tables = self.relation_graph.get_relation(observation)
            relation_tables = list(relation_tables.difference(self.from_space))     # 选过的不选了
            candidate_tables = np.zeros((self.action_space,), dtype=int)
            candidate_tables[relation_tables] = 1
            if len(self.from_space) > 1:
                candidate_tables[self.join] = 1
            else:
                candidate_tables[self.word_num_map['select']] = 1
            return candidate_tables

    def from_action(self, action):
        # print("enter from action")
        if action in self.tables:
            self.from_space.append(action)
        elif action == self.word_num_map['select']:     # 只有一个table
            self.from_clause = self.from_clause + ' ' + self.num_word_map[self.from_space[0]]
            self.cur_state = self.master_control['select']
            self.cur_state[1](action)
        else:
            if action == self.word_num_map['cartesian']:    # 普通的笛卡尔join
                for table_index in self.from_space:
                    self.from_clause = self.from_clause + ' ' + self.num_word_map[table_index] + ','
                self.from_clause = self.from_clause[: -1]
            else:
                join_type = self.num_word_map[action]
                self.from_clause = self.from_clause + ' ' + self.num_word_map[self.from_space[0]]
                for i in range(1, len(self.from_space)):
                    relation_key = self.relation_graph.get_relation_key(self.from_space[i], self.from_space[i - 1])
                    self.from_clause = self.from_clause + ' ' + join_type + ' ' + \
                                       self.num_word_map[self.from_space[i]] + ' on ' + relation_key[0] + '=' + relation_key[1]
            self.cur_state = self.master_control['select']
            self.cur_state[1](action)

        return self.step_reward, 0

    def where_observe(self, observation):
        # print("enter where space")
        candidate_word = np.zeros((self.action_space,), dtype=int)
        if observation == self.word_num_map['where']:
            self.where_attributes = []
            for table_index in self.from_space:
                for field in self.relation_tree.children(table_index):
                    self.where_attributes.append(field.identifier)
            candidate_word[self.where_attributes] = 1
            return candidate_word
        elif observation in self.attributes:
            candidate_word[self.operator] = 1
            # candidate_condition[self.predicate_type] = 1
            return candidate_word
        elif observation in self.operator:
            candidate_word[self.operation_data(self.cur_attribtue)] = 1
            return candidate_word
        elif observation in self.conjunction:
            candidate_word[self.where_attributes] = 1
            return candidate_word
        else:   # data
            candidate_word[self.conjunction] = 1
            self.activate_ternminal(candidate_word)
            self.activate_space(candidate_word, self.word_num_map['order by'])
            if self.group_key:
                self.activate_space(candidate_word, self.word_num_map['having'])
            return candidate_word

        # elif observation in self.predicate_type:

    def where_action(self, action):
        # print("enter where action")
        # print(self.num_word_map[action])
        if action == self.word_num_map['where']:
            self.where_clause = 'where '
        elif action in self.attributes:
            self.cur_attribtue = action
            self.where_clause = self.where_clause + self.num_word_map[action]
        elif action in self.operator:
            self.where_clause = self.where_clause + ' ' + self.num_word_map[action] + ' '
        elif action in self.conjunction:
            self.where_clause = self.where_clause + ' {} '.format(self.num_word_map[action])
        elif action in self.keyword:
            self.cur_state = self.master_control[self.num_word_map[action]]
            self.cur_state[1](action)
        else:   # data
            attribute_type = self.relation_tree.get_node(self.cur_attribtue).data.datatype
            if attribute_type == DataType.VALUE.value:
                self.where_clause = self.where_clause + self.num_word_map[action]
            else:
                self.where_clause = self.where_clause + '\'' + self.num_word_map[action] + '\''
        return self.step_reward, 0

    def operation_data(self, attributes):
        data = [node.data.action_index for node in self.relation_tree.children(attributes)]
        return data

    def group_by_generate(self):
        self.group_by_clause = 'group by'
        for attribute in self.group_by_space:
            self.group_by_clause = self.group_by_clause + ' ' + self.num_word_map[attribute] + ','
        self.group_by_clause = self.group_by_clause[: -1]

    def having_observe(self, observation):
        # self.having_space是聚合函数 + terminal
        candidate_word = np.zeros((self.action_space,), dtype=int)
        cur_word = self.num_word_map[observation]
        if cur_word == 'having':
            aggregate_attributes = [record[1] for record in self.having_space]
            candidate_word[aggregate_attributes] = 1
            return candidate_word
        elif observation in self.attributes:
            self.activate_ternminal(candidate_word)
            self.activate_space(candidate_word, self.word_num_map['order by'])
            if len(self.having_space) != 0:
                candidate_word[self.conjunction] = 1
            return candidate_word
        else:   # 选到连接词
            aggregate_attributes = [record[1] for record in self.having_space]
            candidate_word[aggregate_attributes] = 1
            return candidate_word

    def having_action(self, action):
        # print("having action:", action, "===", self.num_word_map[action])
        if action == self.word_num_map['having']:
            self.having_clause = 'having'
            self.having_space = self.aggregate_space.copy()
        elif action in self.attributes:
            chosen_item = -1
            for item in self.having_space:
                if item[1] == action:
                    chosen_item = item
                    break
            self.having_space.remove(chosen_item)
            assert chosen_item[1] == action
            chosen_operator = np.random.choice(operator)
            chosen_attribute_type = self.relation_tree.get_node(chosen_item[1]).data.datatype

            if chosen_item[0] == 'count':
                chosen_data = np.random.choice(10)
            else:
                chosen_data = np.random.choice(self.operation_data(action))

            self.having_clause = self.having_clause + ' ' + '{aggregate_type}({attribute})'.format(
                aggregate_type=chosen_item[0], attribute=self.num_word_map[chosen_item[1]]) + ' ' + chosen_operator + ' '

            if chosen_item[0] == 'count':
                self.having_clause = self.having_clause + str(chosen_data)
            elif chosen_attribute_type == DataType.VALUE.value:
                self.having_clause = self.having_clause + self.num_word_map[chosen_data]
            else:
                self.having_clause = self.having_clause + '\'' + self.num_word_map[chosen_data] + '\''

        elif action in self.conjunction:
            self.having_clause = self.having_clause + ' ' + self.num_word_map[action]
        else:   # 其他keyword
            self.cur_state = self.master_control[self.num_word_map[action]]
            self.cur_state[1](action)
        return self.step_reward, 0

    def order_by_observe(self, observation):
        candidate_word = np.zeros((self.action_space,), dtype=int)
        if observation == self.word_num_map['order by']:
            self.activate_space(candidate_word, self.word_num_map['select'])    # attribute
            if self.group_key:  #有聚合函数
                self.activate_space(candidate_word, self.word_num_map['aggregate'])
        else:
            self.activate_ternminal(candidate_word)
        return candidate_word

    def order_by_action(self, action):
        if action == self.word_num_map['order by']:
            self.order_by_clause = 'order by'
        elif action == self.word_num_map['select']:
            number = np.random.randint(1, len(self.select_space) + 1)
            attributes = np.random.choice(self.select_space, size=number, replace=False)
            for attribute in attributes:
                choose_order = np.random.choice(order_by_key)
                self.order_by_clause = self.order_by_clause + ' ' + self.num_word_map[attribute] + ' ' + choose_order + ','
        else:   # 'aggregate'
            number = np.random.randint(1, len(self.aggregate_space) + 1)
            tuple_indexes = np.random.choice(range(0, len(self.aggregate_space)), size=number, replace=False)
            for index in tuple_indexes:
                aggregate_tuple = self.aggregate_space[index]
                choose_order = np.random.choice(order_by_key)
                self.order_by_clause = self.order_by_clause + ' ' + '{}({})'.format(aggregate_tuple[0], self.num_word_map[aggregate_tuple[1]]) + ' ' + choose_order + ','
        return self.step_reward, 0

    def add_map(self, series, word_num_map, num_word_map):
        count = len(word_num_map)
        for word in series:
            if word not in word_num_map.keys():
                word_num_map[word] = count
                num_word_map[count] = word
                count += 1

    def observe(self, observation):
        """
        :param observation: index 就可以
        :return: 返回vocabulary_size的矩阵，单步reward
        """
        return self.cur_state[0](observation)

    def step(self, action):
        self.time_step += 1
        if action == 0:  # choose 结束：
            # return self.final_reward(), 1
            return self.get_compare_cardinality_by_json(), 1
        else:
            return self.cur_state[1](action)

    def get_sql(self):
        # print("from clause:", self.from_clause)
        # print('select clause:', self.select_clause)
        # print('aggregate clause:', self.aggregate_clause)
        # print("where_clause:", self.where_clause)
        # print("having clause:", self.having_clause)
        # print("group by clause:", self.group_by_clause)
        # print("order_by_clause clause:", self.order_by_clause)
        final_sql = self.select_clause[: -1]
        if self.aggregate_clause:
            final_sql = final_sql + ', ' + self.aggregate_clause[1: -1]
        final_sql = final_sql + ' ' + self.from_clause
        if self.where_clause:
            final_sql = final_sql + ' ' + self.where_clause
        if self.group_by_clause:
            final_sql = final_sql + ' ' + self.group_by_clause
        if self.having_clause:
            final_sql = final_sql + ' ' + self.having_clause
        if self.order_by_clause:
            final_sql = final_sql + ' ' + self.order_by_clause[: -1]
        final_sql = final_sql + ';'
        return final_sql

    # def get_final_cost(self):
    #     # print('execute sql:', self.cur_sql)
    #     try:
    #         # print('sql:', self.get_sql())
    #         self.cursor.execute('explain' + ' ' + self.get_sql())
    #         result = self.cursor.fetchall()[0][0]
    #         # print(result)
    #         p1 = re.compile(r'[(](.*?)[)]', re.S)
    #         final_cost = re.findall(p1, result)[0].split(' ')[0].split('..')[1]
    #         return 1, int(float(final_cost))
    #     except Exception as result:
    #         self.cursor.close()
    #         self.db.close()
    #         self.db, self.cursor = connect_server(self.dbname, server_name=self.server_name)
    #         print('sql:', self.get_sql())
    #         print("execute fail:", result)
    #         return 0, 0

    def get_evaluate_cost_by_json(self):
        try:
            self.cursor.execute('explain (format json)' + ' ' + self.get_sql())
            evaluate_cost = self.cursor.fetchall()[0][0][0]['Plan']['Total Cost']
            return 1, int(float(evaluate_cost))
        except Exception as result:
            self.cursor.close()
            self.db.close()
            self.db, self.cursor = connect_server(self.dbname, server_name=self.server_name)
            print('sql:', self.get_sql())
            print("execute fail:", result)
            return 0, 0

    def get_evaluate_cardinality_by_json(self):
        try:
            self.cursor.execute('explain (format json)' + ' ' + self.get_sql())
            evaluate_cardinality = self.cursor.fetchall()[0][0][0]['Plan']['Plan Rows']
            return 1, evaluate_cardinality
        except Exception as result:
            self.cursor.close()
            self.db.close()
            self.db, self.cursor = connect_server(self.dbname, server_name=self.server_name)
            print('sql:', self.get_sql())
            print("execute fail:", result)
            return 0, 0

    def get_actual_cardinality_by_json(self):
        try:
            self.cursor.execute('explain (analyze, format json)' + ' ' + self.get_sql())
            actual_cardinality = self.cursor.fetchall()[0][0][0]['Plan']['Actual Rows']
            return 1, actual_cardinality
        except Exception as result:
            self.cursor.close()
            self.db.close()
            self.db, self.cursor = connect_server(self.dbname, server_name=self.server_name)
            print('sql:', self.get_sql())
            print("execute fail:", result)
            return 0, 0

    def get_compare_cardinality_by_json(self):
        try:
            self.cursor.execute('explain (analyze, format json)' + ' ' + self.get_sql())
            res = self.cursor.fetchall()[0][0][0]['Plan']
            evaluate_cardinality = res['Plan Rows']
            actual_cardinality = res['Actual Rows']
            return 1, (evaluate_cardinality, actual_cardinality)
        except Exception as result:
            self.cursor.close()
            self.db.close()
            self.db, self.cursor = connect_server(self.dbname, server_name=self.server_name)
            print('sql:', self.get_sql())
            print("execute fail:", result)
            return 0, 0

    def cost2reward(self, cost):
        log_cost = 0
        if cost != 0:  # 有的cost为0因为filter能直接能够判定这次查询无结果
            log_cost = math.log(cost, 1.5)
        dist = abs(log_cost - self.log_target)
        # print('reward:', -dist)
        return 1 / dist - 0.45

    def cardinality2reward(self, cardinality):
        # to do
        return 1

    def final_reward(self):
        # print(sql)
        if self.target_type == 0:
            success, cost = self.get_evaluate_cost_by_json()
            if success:
                return self.cost2reward(cost)
        else:
            success, cardinality = self.get_evaluate_cardinality_by_json()
            if success:
                return self.cardinality2reward(cardinality)
        exit()
        return self.bug_reward

    def __del__(self):
        self.cursor.close()
        self.db.close()


def choose_action(observation):
    candidate_list = np.argwhere(observation == np.max(observation)).flatten()
    # action = np.random.choice(candidate_list, p=increase_key_probability(candidate_list, key_word_list, step))
    action = np.random.choice(candidate_list)
    return action


SEQ_LENGTH = 20


def test_generate():
    env = GenSqlEnv(metric=100000, dbname='tpch', target_type=0)
    episode = 0
    max_episodes = 10000
    while episode < max_episodes:
        # print('第', episode, '条')
        current_state = env.reset()
        reward, done = env.bug_reward, False
        ep_steps = 0
        while not (done or ep_steps >= SEQ_LENGTH):
            action = choose_action(env.observe(current_state))
            reward, done = env.step(action)
            ep_steps += 1
            current_state = action
        if ep_steps == SEQ_LENGTH or reward == env.bug_reward:
            print('采样忽略')
        else:
            episode += 1
            print(env.get_sql())
            # print('reward:', reward)


if __name__ == '__main__':
    test_generate()
    # db, cursor = connect_server(dbname='tpch', server_name='postgresql')
    # cursor.execute('explain (format json) select * from customer where c_nationkey<2')
    # result = cursor.fetchall()[0][0][0]['Plan']['Total Cost']
    # print(result)












