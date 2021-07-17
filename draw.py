
from graphviz import Digraph


def traverse(g, node, pid):
    cid = pid
    for c in node.children:
        cid = cid + 1
        if c.selected == 1:
            g.node('node'+str(cid), label='reward:' + str(c.reward), color = 'red')
        else:
            g.node('node' + str(cid), label='reward:' + str(c.reward))
        g.edge('node'+str(pid), 'node'+str(cid))
        if len(c.children) != 0:
            g, cid = traverse(g, c, cid)

    return g, cid

def drawPolicyTree(root, filename):

    g = Digraph('G', filename='./queries/input_sql/'+filename + '.gv')

    g.node('node1', label='reward:'+str(root.reward))

    g, num_nodes = traverse(g, root, 1)

    print("num_nodes:"+str(num_nodes))

    g.view()