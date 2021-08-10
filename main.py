#!/usr/bin/env python

import random
import math
import hashlib
import logging
import argparse

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
from rewriter import rewrite
from configs import parse_cmd_args
from database import Database, RewriteState
from rewriterV3 import rewrite
from cost_estimator import previous_cost_estimation

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

    sql = args.sql
    # s1: the origin cost (baseline) -- baseline
    # print(sql)
    origin_cost = previous_cost_estimation(sql, db)
    # print("origin cost: "+str(origin_cost))

    rewritten_sql = rewrite(args, db, origin_cost) # mcts/topdown

    rewritten_cost = previous_cost_estimation(rewritten_sql, db)
    print("after-rewrite cost: "+str(rewritten_cost))

    print("-------")


if __name__ == "__main__":
    rewrite_sql()