#!/usr/bin/env python
import random
import math
import hashlib
import logging
import argparse

from parallel_algorithm import parallel_node_selection

from cost_estimator import previous_cost_estimation

"""
A quick Monte Carlo Tree Search implementation.  For more details on MCTS see See http://pubs.doc.ic.ac.uk/survey-mcts-methods/survey-mcts-methods.pdf

The State is just a game where you have NUM_TURNS and at turn i you can make
a choice from [-2,2,3,-3]*i and this to to an accumulated value.  The goal is for the accumulated value to be as close to 0 as possible.

The game is not very interesting but it allows one to study MCTS which is.  Some features 
of the example by design are that moves do not commute and early mistakes are more costly.  

In particular there are two models of best child that one can use 
"""

# MCTS scalar.  Larger scalar will increase exploitation, smaller will increase exploration.
SCALAR=1/math.sqrt(2.0)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger('MyLogger')

class Node():
	def __init__(self, sql, db, origin_cost, rewriter, gamma, parent=None):
		self.visits = 1
		self.state = sql
		self.reward = origin_cost - previous_cost_estimation(sql, db) # initialize with previous cost reduction
		#print("previous reward: " + str(self.reward))
		self.rewriter = rewriter
		self.children=[]
		self.parent=parent
		self.rewrite_sequence = []
		self.node_num = 1

		# for multiple node selection
		self.non_computed = 1
		self.selected_nodes = []
		self.selected_nodes_utilities = []

		self.db = db
		self.origin_cost = origin_cost
		self.gamma = gamma
		self.selected = 0

	def add_child(self, csql, db, origin_cost, rewriter, rule_id):
		child = Node(csql, db, origin_cost, rewriter, self.gamma, self)
		child.rewrite_sequence = self.rewrite_sequence + [rule_id]

		if self.children == None:
			self.children = []
		self.children.append(child)

	def update(self,reward):
		self.reward+=reward
		self.visits+=1
	def __repr__(self):
		s="Node; children: %s; visits: %d; reward: %f"%(self.children,self.visits,self.reward)
		return s

	def is_terminal(self):
		# self.sql cannot be rewritten by any rules
		if self.children != None and len(self.children) > 0:
			return False

		return True

	def	node_children(self):
		for i in range(len(self.rewriter.rulelist)):
			# print("success rewrite")
			is_rewritten, csql = self.rewriter.single_rewrite(self.state, i)

			if is_rewritten == 1 and previous_cost_estimation(csql, self.db)<self.origin_cost:

				self.add_child(csql, self.db, self.origin_cost, self.rewriter, i)

def UCTSEARCH(budget,root, parallel_num):

	root.selected = 1
	root.node_children()  # first expand root's children
	for c in root.children:
		c.parent = root

	for iter in range(int(budget)):
		# if iter%20==19:
		# 	logger.info("simulation: %d"%iter)
		# 	logger.info(root)
		if parallel_num == 1 or parallel_num > root.node_num: # select single node
			front = TREEPOLICY(root) # node selection
			front_list = [front]
		else:
			front_list = parallel_node_selection(root,parallel_num)

		for front in front_list:
			front.selected = 1

			front.node_children()  # expansion
			for c in front.children:
				c.parent = front
			root.node_num = root.node_num + len(front.children)

			reward = DEFAULTPOLICY(front) # estimation (simplified as the future cost reduction by default rewrite order)
			# print("sub reward: " + str(reward))
			if reward > front.reward:
				BACKUP(front,reward) # backpropagation

	# return the node with maximal utility
	best_node = root
	while best_node.is_terminal() == False:
		bestchildren = [best_node.children[0]]
		bestscore = best_node.children[0].reward
		for c in best_node.children:
			#if c.reward == bestscore:  # utility
			#	bestchildren.append(c)
			if c.reward > bestscore:
				bestchildren = [c]
				bestscore = c.reward

		best_node = random.choice(bestchildren)

	return best_node

'''
	best_node = root
	while best_node.is_terminal() == False:
		bestchildren = [best_node.children[0]]
		bestscore = best_node.children[0].reward
		for c in best_node.children:

			if c.reward == bestscore:  # utility
				bestchildren.append(c)
			if c.reward > bestscore:
				bestchildren = [c]
				bestscore = c.reward

		random.choice(bestchildren)
'''

def TREEPOLICY(node):

	while node.is_terminal()==False:

		node=BESTCHILD(node) # highest utility

	return node

#current this uses the most vanilla MCTS formula it is worth experimenting with THRESHOLD ASCENT (TAGS)
def BESTCHILD(node):
	bestscore=node.children[0].reward
	bestchildren=[node.children[0]]
	for c in node.children:
		# utility function
		exploit=c.reward/c.visits
		explore=math.sqrt(2.0*math.log(node.visits)/float(c.visits))
		score=exploit+node.gamma*explore

		if score==bestscore: # utility
			bestchildren.append(c)
		if score>bestscore:
			bestchildren=[c]
			bestscore=score
	# if len(bestchildren)==0:
	# 	logger.warn("OOPS: no best child found, probably fatal")

	return random.choice(bestchildren)

def DEFAULTPOLICY(selected_node):
	# random sample from the selected node
	is_rewritten, sampled_sql = selected_node.rewriter.single_rewrite(selected_node.state, -1)

	if is_rewritten == 1:
		return selected_node.origin_cost - previous_cost_estimation(sampled_sql, selected_node.db)
	else:
		return -1

def BACKUP(node,reward):
	while node!=None:
		node.visits+=1
		if reward > node.reward:
			node.reward = reward
		node=node.parent # verified
	return