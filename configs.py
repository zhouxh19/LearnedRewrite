# -*- coding: utf-8 -*-
"""
desciption: system variables or other constant information
"""

import os
import requests
import argparse
import math

def parse_cmd_args():

    parser = argparse.ArgumentParser(description='Query Rewrite (Policy Tree Search)')

    # database
    parser.add_argument('--host', type=str, default='166.111.121.62', help='Host IP Address')
    parser.add_argument('--dbname', type=str, default='tpch1x', help='Database Name')
    parser.add_argument('--port', type=int, default=5432, help='Host Port Number')
    parser.add_argument('--user', type=str, default='postgres', help='Database User Name')
    parser.add_argument('--password', type=str, default='postgres', help='Database Password')
    # server
    parser.add_argument('--hostuser', type=str, default='xuanhe', help='Host User Name')
    parser.add_argument('--hostpassword', type=str, default='db10204', help='Host Password')

    # rewrite
    parser.add_argument('--rewrite_policy', type=str, default='mcts', help='[mcts, topdown, arbitrary, heuristic]')
    parser.add_argument('--driver', type=str, default='org.postgresql.Driver', help='Calcite adapter')
    parser.add_argument('--sql', type=str, help='input query')
    parser.add_argument('--parallel_num', type=int, default=1, help='Parallel Node Number')

    # mcts
    parser.add_argument('--num_turns', action="store", type=int, default=1,
                        help="Number of turns to run")
    parser.add_argument('--num_sims', action="store", type=int, default=10,
                        help="Number of simulations to run")
    parser.add_argument('--gamma', type=float, default=1/math.sqrt(2.0), help='Rate of explorations of uncovered rewrite orders')


    args = parser.parse_args()
    #argus = vars(args)

    return args
