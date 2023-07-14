import itertools
import os
import sys

from graphframes import GraphFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell")

filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
output_file_path = sys.argv[3]

configuration = SparkConf()
configuration.set("spark.driver.memory", "4g")
configuration.set("spark.executor.memory", "4g")
sc = SparkContext.getOrCreate(configuration)
sparkSession = SparkSession(sc)

# Schema: ('user_id', 'business_id')
rdd = sc.textFile(input_file_path).map(lambda line: line.split(','))

header = rdd.first()

# Schema: ('user_id': {'business_id1', 'business_id2', 'business_id3', ...})
# i.e. key: user_id value: set of its corresponding business_id
uid_bid_dict = rdd.filter(lambda row: row != header).groupByKey(
).mapValues(lambda b_ids: set(list(b_ids))).collectAsMap()

# Given a pair, return the number of common businesses


def num_common_busi(pair):
    set1 = uid_bid_dict[pair[0]]
    set2 = uid_bid_dict[pair[1]]
    return len(set1.intersection(set2))


# Schema: [('user_id1', 'user_id2'), ('user_id1', 'user_id3')......
# i.e. user_id pairs
uid_pairs = list(itertools.combinations(list(uid_bid_dict.keys()), 2))

# Get all edges in a list
# Schema: [edge1, edge2, edge3, ...]
edge_lst = list()
vertex_set = set()
for pair in uid_pairs:
    if num_common_busi(pair) >= filter_threshold:
        # If A-B is an edge, We include both (A, B) and (B, A) in this list
        edge_lst.append(tuple(pair))
        edge_lst.append(tuple((pair[1], pair[0])))
        vertex_set.add(pair[0])
        vertex_set.add(pair[1])

edges = sc.parallelize(edge_lst).toDF(["src", "dst"])

keys = list(vertex_set)
vertices_lst = [(ele, )for ele in keys]

vertices = sc.parallelize(vertices_lst).toDF(['id'])

g = GraphFrame(vertices, edges)

result = g.labelPropagation(maxIter=5)

result_list = result.rdd.map(lambda row: (row[1], row[0])).groupByKey().map(lambda row: sorted(list(row[1]))) \
    .sortBy(lambda row: (len(row), row[0])).collect()

with open(output_file_path, 'w') as f:
    for ele in result_list:
        f.write(f"{str(ele)[1:-1]}\n")
