
import math
import random


class traverse(object):
    def levelOrder(self, root, select_num):
        """
        :type root: Node
        :rtype: List[List[int]]
        """
        if not root:
            return []
        que = [] # 保存节点的队列
        res = [] # 保存结果的列表
        que.append(root) # 根元素入队列
        while len(que): # 判断队列不为空
            l = len(que)
            sub = [] # 保存每层的节点的值
            for i in range(l):
                current = que.pop(0) # 出队列的当前节点
                if current.children == []:
                    current.non_computed = 0
                    current.selected_nodes = [current]
                    current.selected_nodes_utilities = [current.reward]
                    current.selected_nodes = [[] for i in range(1,select_num)]
                    current.selected_nodes_utilities = [0 for i in range(1,select_num)]

                    sub.append(current) # current_node non_computed selected_nodes selected_nodes_utility

                else:
                    current.non_computed = 1
                    current.selected_nodes = [[] for i in range(select_num)]
                    current.selected_nodes_utilities = [0 for i in range(select_num)]

                    sub.append(current) # current_node non_computed selected_nodes selected_nodes_utility

                for child in current.children: # 所有子结点入队列
                    que.append(child)

            res.append(sub) # 把每层的节点的值加入结果列表


        return res

    def reset(self, root):
        if not root:
            return []

        que = []  # 保存节点的队列
        que.append(root)  # 根元素入队列
        while len(que):  # 判断队列不为空

            for i in range(len(que)):
                current = que.pop(0)  # 出队列的当前节点
                current.non_computed = 0
                current.selected_nodes = []
                current.selected_nodes_utilities = []

                for child in current.children:  # 所有子结点入队列
                    que.append(child)

def parallel_node_selection(root, parallel_num):

    # level traversal
    traverser = traverse()
    node_list = traverser.levelOrder(root,parallel_num)
    node_list = node_list[::-1]

    # assign (value, nodes) for non-leaf nodes
    for node in node_list[::-1]:
        # node, non_computed, selected_nodes, selected_nodes_utilities = node_array
        if node.non_computed == 1:
            # construct DP matrix and correpsonding solutions
            child_list = []
            for child in node.children:
                child_list.append(child)

            utility_matrix = []
            for i in range(parallel_num):
                nw = [0 for i in range(len(node.children))]
                utility_matrix.append(nw)

            selected_node_matrix = []
            for i in range(parallel_num):
                nw = [[] for i in range(len(node.children))]
                selected_node_matrix.append(nw)

            utility_matrix[0][0] = child_list[0].reward
            selected_node_matrix[0][0] = child_list[0]

            for i in range(1,len(node.children)):
                if utility_matrix[0][i-1] >= child_list[i].reward:
                    utility_matrix[0][i] = utility_matrix[0][i-1]
                    selected_node_matrix[0][i] = selected_node_matrix[0][i-1]
                else:
                    utility_matrix[0][i] = child_list[i].reward
                    selected_node_matrix[0][i] = [child_list[i]]

            if node.reward >= utility_matrix[0][len(node.children)-1]:
                node.selected_nodes_utilities[0] = node.reward
                node.selected_nodes[0] = [node]
            else:
                node.selected_nodes_utilities[0] = utility_matrix[0][len(node.children)-1]
                node.selected_nodes[0] = selected_node_matrix[0][len(node.children)-1]


            for i in range(1,parallel_num):
                for j in range(len(node.children)):
                    if j > 0:
                        s1 = utility_matrix[0][j-1]
                    else:
                        s1 = 0

                    s2 = child_list[j].selected_nodes_utilities[i]
                    s3 = utility_matrix[i-1][j-1] + child_list[j].selected_nodes_utilities[1]
                    max_z = 1
                    for z in range(1,i):
                        if s3 < utility_matrix[i-z][j-1] + child_list[j].selected_nodes_utilities[z]:
                            max_z = z
                            s3 = utility_matrix[i-z][j-1] + child_list[j].selected_nodes_utilities[z]

                    # utility_matrix[i][j] = max(s1, s2, s3)
                    if j>0 and s1>=s2 and s1>=s3:
                        utility_matrix[i][j] = s1
                        selected_node_matrix[i][j] = selected_node_matrix[i][j-1]
                    elif s2>=s1 and s2>=s3:
                        utility_matrix[i][j] = s2
                        selected_node_matrix[i][j] = child_list[j].selected_nodes[i]
                    else:
                        utility_matrix[i][j] = s3
                        selected_node_matrix[i][j] = selected_node_matrix[i-max_z][j-1] + child_list[j].selected_nodes[max_z]

                node.selected_nodes_utilities[i] = utility_matrix[i][len(node.children)-1]
                node.selected_nodes[i] = selected_node_matrix[i][len(node.children)-1]

    selected_nodes = root.selected_nodes[parallel_num - 1]

    traverser.reset(root)

    return selected_nodes


# return the best solution in root node
'''
bestscore = root.children[0].reward
bestchildren = [root.children[0]]
for c in root.children:
    # utility function
    exploit = c.reward / c.visits
    explore = math.sqrt(2.0 * math.log(root.visits) / float(c.visits))
    score = exploit + root.gamma * explore

    if score == bestscore:  # utility
        bestchildren.append(c)
    if score > bestscore:
        bestchildren = [c]
        bestscore = score
# if len(bestchildren)==0:
# 	logger.warn("OOPS: no best child found, probably fatal")    
'''

