# Learned Query Rewrite System using Monte Carlo Tree Search

To address challenges in classic query rewrite strategies (e.g., topdown rewrite from the root node), we propose a policy tree based  query rewrite framework, where the root is the input query and each node is a rewritten query from its parent. We aim to explore the tree nodes in the \tree to find the optimal rewrite query. We propose to use Monte Carlo Tree Search to explore the policy tree, which navigates the policy tree to efficiently get the optimal node. Moreover, we propose a learning-based model to  estimate the expected performance improvement of each rewritten query, which guides the tree search more accurately.  We also propose a parallel algorithm that can explore the tree search in parallel in order to improve the performance.  



## Before you run

You need to configure `java` and `maven` environment properly.

Install dependencies.

```bash
pip3 install -r requirements.txt
```

## How to run

1. Support two types of input queries:

- Save your `k` SQLs as k documents: ./queries/input_sql/1.sql - ./queries/input_sql/k.sql 


- Directly run the benchmark queries and nothing to do here 

2. Start jupyter notebook:

   `jupyter lab` 

3. Run the `LearnedRewrite.ipynb` script in jupyter, which reads to-rewrite queries, calls the main.py script to rewrite the queries, and buffers the rewrite results in rewrite_set (in json format).

## Estimation Model

1. Training Data Generation (./queries/input_sql)



2. Model Design



3. The results of Model Training

   

## TODO

- Fix problem in learned cost estimation model

- Support more rewrite strategies

- Support complex SQL syntax.
