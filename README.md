# Learned Query Rewrite System using Monte Carlo Tree Search

To address challenges in classic query rewrite strategies (e.g., topdown rewrite from the root node), we propose a policy tree based  query rewrite framework, where the root is the input query and each node is a rewritten query from its parent. We aim to explore the tree nodes in the \tree to find the optimal rewrite query. We propose to use Monte Carlo Tree Search to explore the policy tree, which navigates the policy tree to efficiently get the optimal node. Moreover, we propose a learning-based model to  estimate the expected performance improvement of each rewritten query, which guides the tree search more accurately.  We also propose a parallel algorithm that can explore the tree search in parallel in order to improve the performance.  


# Online Demo

http://rewrite_demo.dbmind.cn/


# Before you run

You need to configure `java` and `maven` environment properly.

Install dependencies.


# Citing LearnedRewrite

```bibTex
@article{DBLP:journals/pvldb/ZhouLCF21,
  author    = {Xuanhe Zhou and
               Guoliang Li and
               Chengliang Chai and
               Jianhua Feng},
  title     = {A Learned Query Rewrite System using Monte Carlo Tree Search},
  journal   = {Proc. {VLDB} Endow.},
  volume    = {15},
  number    = {1},
  pages     = {46--58},
  year      = {2021},
  url       = {http://www.vldb.org/pvldb/vol15/p46-li.pdf},
  timestamp = {Tue, 11 Jan 2022 18:01:10 +0100},
  biburl    = {https://dblp.org/rec/journals/pvldb/ZhouLCF21.bib},
  bibsource = {dblp computer science bibliography, https://dblp.org}
}
````

