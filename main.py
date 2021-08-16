<<<<<<< HEAD
#!/usr/bin/env python

import random
import math
import hashlib
import logging
import argparse
import json
import ipdb

import sys
import os
import sqlparse
import time

import jpype as jp
from jpype.types import *
import jpype.imports

from mcts import *
from configs import parse_cmd_args
from database import Database, RewriteState
from rewriterV3 import rewrite
from cost_estimator import previous_cost_estimation

def analyse_part(json_sql, list_node, list_cost):
    for i in json_sql.keys():
        if i == "Node Type":
            list_node.append(json_sql[i])
        elif i == "Total Cost":
            list_cost.append(json_sql[i] - json_sql['Startup Cost'])
        elif i == "Plans":
            for u in json_sql[i]:
                analyse_part(u, list_node, list_cost)

def analyse(json_sql, list_node, list_cost):
    for i in json_sql.keys():
        if i == 'Plan':
            analyse_part(json_sql[i], list_node, list_cost)

"""
Another game using MCTS. based on a comment from 
atanas1054 on Jun 27, 2017

I want to have a 2-player game where they take turns. In the beginning there are 114 possible actions and they decrease by 1 every time a player makes a move. The game is played for 10 turns (that's the terminal state). I have my own function for the reward.

Here is a sample game tree:

START- available actions to both players -> [1,2,3,4,5,6....112,113,114]
Player 1 - takes action 5 -> [5,0,0,0,0,0,0,0,0,0] -remove action 5 from available actions
Player 2 - takes action 32->[5,0,0,0,0,32,0,0,0,0] - remove action 32 from the available actions
Player 1- takes action 97 ->[5,97,0,0,0,32,0,0,0,0] - remove action 97 from the available actions
Player 2 takes action 56 -> [5,97,0,0,0,32,56,0,0,0] - remove action 56 from the available actions
....
Final (example) game state after each player makes 5 actions -> [5,97,3,5,1,32,56,87,101,8]
First 5 entries present the actions taken by Player1, second 5 entries present the actions taken by Player 2

Finally, I apply a reward function to this vector [5,97,3,5,1,32,56,87,101,8]
"""

def rewrite_sql():

    args = parse_cmd_args()
    print("Connecting..." + "\n")
    db = Database(args)

    base_dir = os.path.abspath(os.curdir)
    local_lib_dir = os.path.join(base_dir, 'libs')

    rewrite_dir = os.path.join(base_dir, 'queries', 'input_sql')

    sql_list = []
    #with open("./database_gen_queries/1_queries.log", "r") as sql_file:
    #    tmp_sql = ""
    #    c = sql_file.readline()
    #    while c != "":
    #        while len(c) <= 1 or c[-2] != ';':
    #            tmp_sql += c[:-1] + " "
    #            c = sql_file.readline()
    #            # print(c)
    #        tmp_sql += c[:-1] + " "
    #        sql_list.append(tmp_sql)
    #        tmp_sql = ""
    #        c = sql_file.readline()
    with open("./lixi_sqls.log",'r') as sql_file:
        for _ in sql_file:
            sql_list.append(_[:-1])
    for cnt, sql in enumerate(sql_list):  
        try:
            print(str(cnt) + ".....\n" + str(sql))
            # sql = sql
            newsql = list(sql)
            # sql1 = sql
            # s1: the origin cost (baseline) -- baseline
            # origin_cost = previous_cost_estimation(sql, db)
            # print("origin cost: "+str(origin_cost))

            # rewritten_sql = rewrite(args, db, origin_cost, sql1) # mcts/topdown

            cur = db.return_cursor()
            cur.execute('explain (FORMAT JSON) '+ sql)
            res = cur.fetchall()
            r = res[0][0][0]

            with open("f.json",'w') as out_file:
                out_file.write( json.dumps(str(res[0][0][0])) )

            list_node = []
            list_cost = []
            analyse(res[0][0][0], list_node, list_cost)
            # ipdb.set_trace()
            # rewritten_cost = previous_cost_estimation(rewritten_sql, db)
            with open("2queries_output_file.txt", "a+") as list_out:
                list_out.write(str(list_node) + "\n")
                list_out.write(str(list_cost) + "\n")
                list_out.write(str(res[0][0][0]["Plan"]["Total Cost"] - res[0][0][0]["Plan"]["Startup Cost"]) + "\n")
            
            # print("after-rewrite cost: "+str(rewritten_cost))

            print("-------")
        except:
            pass


if __name__ == "__main__":
=======
#!/usr/bin/env python

import random
import math
import hashlib
import logging
import argparse
import json

import sys
import os
import sqlparse
import time

import jpype as jp
from jpype.types import *
import jpype.imports

from mcts import *
from configs import parse_cmd_args
from database import Database, RewriteState
from rewriterV3 import rewrite
from cost_estimator import previous_cost_estimation

def analyse_part(json_sql, list_node, list_cost):
    for i in json_sql.keys():
        if i == "Node Type":
            list_node.append(json_sql[i])
        elif i == "Total Cost":
            list_cost.append(json_sql[i] - json_sql['Startup Cost'])
        elif i == "Plans":
            for u in json_sql[i]:
                analyse_part(u, list_node, list_cost)

def analyse(json_sql, list_node, list_cost):
    for i in json_sql.keys():
        if i == 'Plan':
            analyse_part(json_sql[i], list_node, list_cost)

"""
Another game using MCTS. based on a comment from 
atanas1054 on Jun 27, 2017

I want to have a 2-player game where they take turns. In the beginning there are 114 possible actions and they decrease by 1 every time a player makes a move. The game is played for 10 turns (that's the terminal state). I have my own function for the reward.

Here is a sample game tree:

START- available actions to both players -> [1,2,3,4,5,6....112,113,114]
Player 1 - takes action 5 -> [5,0,0,0,0,0,0,0,0,0] -remove action 5 from available actions
Player 2 - takes action 32->[5,0,0,0,0,32,0,0,0,0] - remove action 32 from the available actions
Player 1- takes action 97 ->[5,97,0,0,0,32,0,0,0,0] - remove action 97 from the available actions
Player 2 takes action 56 -> [5,97,0,0,0,32,56,0,0,0] - remove action 56 from the available actions
....
Final (example) game state after each player makes 5 actions -> [5,97,3,5,1,32,56,87,101,8]
First 5 entries present the actions taken by Player1, second 5 entries present the actions taken by Player 2

Finally, I apply a reward function to this vector [5,97,3,5,1,32,56,87,101,8]
"""

def rewrite_sql():

    args = parse_cmd_args()
    db = Database(args)

    base_dir = os.path.abspath(os.curdir)
    local_lib_dir = os.path.join(base_dir, 'libs')

    rewrite_dir = os.path.join(base_dir, 'queries', 'input_sql')

    sql_list = []
    #with open("./database_gen_queries/1_queries.log", "r") as sql_file:
    #    tmp_sql = ""
    #    c = sql_file.readline()
    #    while c != "":
    #        while len(c) <= 1 or c[-2] != ';':
    #            tmp_sql += c[:-1] + " "
    #            c = sql_file.readline()
    #            # print(c)
    #        tmp_sql += c[:-1] + " "
    #        sql_list.append(tmp_sql)
    #        tmp_sql = ""
    #        c = sql_file.readline()
    with open("./lixi_new_sqls.log",'r') as sql_file:
        for _ in sql_file:
            sql_list.append(_[:-1])
    for cnt, sql in enumerate(sql_list):  
        try:
            # print(str(cnt) + ".....\n" + str(sql))
            # sql = sql
            newsql = list(sql)
            # sql1 = sql
            # s1: the origin cost (baseline) -- baseline
            # origin_cost = previous_cost_estimation(sql, db)
            # print("origin cost: "+str(origin_cost))

            # rewritten_sql = rewrite(args, db, origin_cost, sql1) # mcts/topdown

            cur = db.return_cursor()
            cur.execute('explain (FORMAT JSON) '+ sql)
            res = cur.fetchall()
            r = res[0][0][0]

            with open("f.json",'w') as out_file:
                out_file.write( json.dumps(str(res[0][0][0])) )

            list_node = []
            list_cost = []
            analyse(res[0][0][0], list_node, list_cost)
            # rewritten_cost = previous_cost_estimation(rewritten_sql, db)
            with open("2queries_output_file.txt", "a+") as list_out:
                list_out.write(str(list_node) + "\n")
                list_out.write(str(list_cost) + "\n")
                list_out.write(str(res[0][0][0]["Plan"]["Cost"]) + "\n")
            
            print("after-rewrite cost: "+str(rewritten_cost))

            print("-------")
        except:
            pass


if __name__ == "__main__":
>>>>>>> 1d2895a... update
    rewrite_sql()