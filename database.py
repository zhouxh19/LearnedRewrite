<<<<<<< HEAD
import random
import math
import hashlib
import logging
import argparse

import psycopg2


class RewriteState():

    def __init__(self, args, sql, num_rules):

        self.current = [0] * 2 * args.num_turns
        self.turn = 0
        self.num_moves = (114 - self.turn) * (114 - self.turn - 1)
        self.args = args
        self.num_rules = num_rules

    def next_state(self):
        availableActions = [x for x in range(self.num_rules)]
        for c in self.current:
            if c in availableActions:
                availableActions.remove(c)

        player1action = random.choice(availableActions)
        availableActions.remove(player1action)
        nextcurrent = self.current[:]
        nextcurrent[self.turn] = player1action
        player2action = random.choice(availableActions)
        availableActions.remove(player2action)
        nextcurrent[self.turn + self.args.num_turns] = player2action
        next = RewriteState(self.args)

        return next

    def terminal(self):
        if self.turn == self.args.num_turns:
            return True
        return False

    def reward(self):
        r = random.uniform(0, 1)  # ANTAS, put your own function here
        return r

    def __hash__(self):
        return int(hashlib.md5(str(self.current).encode('utf-8')).hexdigest(), 16)

    def __eq__(self, other):
        if hash(self) == hash(other):
            return True
        return False

    def __repr__(self):
        s = "CurrentState: %s; turn %d" % (self.current, self.turn)
        return s


class Database():

    def __init__(self, args):
        self.conn = psycopg2.connect(database=args.dbname,  # tpch1x (0.1m, 10m), tpch100m (100m)
                                user=args.user,
                                password=args.password,
                                host=args.host,
                                port=args.port)

    def execute_sql(self, sql):
        fail = 1
        cur = self.conn.cursor()

        i=0
        cnt = 3
        while fail == 1 and i<cnt:
            try:
                fail = 0
                cur.execute('explain (FORMAT JSON) '+sql)
            except:
                fail = 1
            res = []
            if fail == 0:
                res = cur.fetchall()
            i = i + 1

        if fail == 1:
            print("SQL Execution Fatal!!")
            return 0, ''
        elif fail == 0:
            return 1, res
    def return_cursor(self):
        return self.conn.cursor()
    # query cost estimated by the optimizer
    def cost_estimation(self, sql):
        success, res = self.execute_sql(sql)
        print(success, res)
        if success == 1:
            cost = res[0][0][0]['Plan']['Total Cost']

            return cost
        else:
            return 100000000000
=======
import random
import math
import hashlib
import logging
import argparse

import psycopg2


class RewriteState():

    def __init__(self, args, sql, num_rules):

        self.current = [0] * 2 * args.num_turns
        self.turn = 0
        self.num_moves = (114 - self.turn) * (114 - self.turn - 1)
        self.args = args
        self.num_rules = num_rules

    def next_state(self):
        availableActions = [x for x in range(self.num_rules)]
        for c in self.current:
            if c in availableActions:
                availableActions.remove(c)

        player1action = random.choice(availableActions)
        availableActions.remove(player1action)
        nextcurrent = self.current[:]
        nextcurrent[self.turn] = player1action
        player2action = random.choice(availableActions)
        availableActions.remove(player2action)
        nextcurrent[self.turn + self.args.num_turns] = player2action
        next = RewriteState(self.args)

        return next

    def terminal(self):
        if self.turn == self.args.num_turns:
            return True
        return False

    def reward(self):
        r = random.uniform(0, 1)  # ANTAS, put your own function here
        return r

    def __hash__(self):
        return int(hashlib.md5(str(self.current).encode('utf-8')).hexdigest(), 16)

    def __eq__(self, other):
        if hash(self) == hash(other):
            return True
        return False

    def __repr__(self):
        s = "CurrentState: %s; turn %d" % (self.current, self.turn)
        return s


class Database():

    def __init__(self, args):
        self.conn = psycopg2.connect(database=args.dbname,  # tpch1x (0.1m, 10m), tpch100m (100m)
                                user=args.user,
                                password=args.password,
                                host=args.host,
                                port=args.port)

    def execute_sql(self, sql):
        fail = 1
        cur = self.conn.cursor()

        i=0
        cnt = 3
        while fail == 1 and i<cnt:
            try:
                fail = 0
                cur.execute('explain (FORMAT JSON) '+sql)
            except:
                fail = 1
            res = []
            if fail == 0:
                res = cur.fetchall()
            i = i + 1

        if fail == 1:
            print("SQL Execution Fatal!!")
            return 0, ''
        elif fail == 0:
            return 1, res
    def return_cursor(self):
        return self.conn.cursor()
    # query cost estimated by the optimizer
    def cost_estimation(self, sql):
        success, res = self.execute_sql(sql)
        print(success, res)
        if success == 1:
            cost = res[0][0][0]['Plan']['Total Cost']

            return cost
        else:
            return 100000000000
>>>>>>> 1d2895a... update
