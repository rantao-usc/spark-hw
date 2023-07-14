import sys

from pyspark import SparkContext, SparkConf

import collections
from collections import defaultdict
from collections import deque
import itertools
import copy


def flatten(l):
    return [item for sublist in l for item in sublist]

# Generate a BFS tree given a root node and then compute the edge betweenness


def compute_edgeBetweenness(root, nodes, pair_value_dict, graph):
    # dist: the shortest distances from source vertex
    # initially all the elements are infinity except source vertex which is equal to 0
    dist_dict = dict.fromkeys(nodes, len(nodes))
    dist_dict[root] = 0

    # the number of different shortest paths from the source vertex to this vertex
    # all the elements are 0 except source vertex which is equal to 1
    num_of_SH_dict = dict.fromkeys(nodes, 0)
    num_of_SH_dict[root] = 1

    # for each vertex, its former node(s) in the shortest path
    former_nodes_dict = defaultdict(list)

    # for each node, whether it is visited or not
    visited = set()

    # initiating value of each node is 1
    value_dict = dict.fromkeys(nodes, 1)

    # Root Node
    q = deque()
    q.append(root)
    visited.add(root)
    former_nodes_dict[root] = []

    while q:
        curr = q[0]
        q.popleft()

    # For all neighbors of current vertex do:
    for node in graph[curr]:

        # if the current vertex is not yet
        # visited, then push it to the queue.
        if node not in visited:
            q.append(node)
            visited.add(node)

        # check if there is a better path.
        if dist_dict[node] > dist_dict[curr] + 1:
            dist_dict[node] = dist_dict[curr] + 1
            num_of_SH_dict[node] = num_of_SH_dict[curr]
            former_nodes_dict[node].append(curr)

        # additional shortest paths found
        elif dist_dict[node] == dist_dict[curr] + 1:
            num_of_SH_dict[node] += num_of_SH_dict[curr]
            former_nodes_dict[node].append(curr)

    dist_dict = {k: v for k, v in dist_dict.items() if k in visited}

    # First do some operations on nodes of dist 3, then dist 2, ....
    # 一层一层往上遍历
    # Scheme example: {3: ['A', 'C'], 2: ['B', 'G'], 1: ['D', 'F'], 0: ['E']}
    groupByDist_dict = defaultdict(list)
    for x, y in dist_dict.items():
        groupByDist_dict[y].append(x)

    traverse_list = flatten(list(collections.OrderedDict(
        sorted(groupByDist_dict.items(), reverse=True)).values()))

    for vertex in traverse_list[:-1]:
        # print(vertex)
        # update the value of its former node
        formerNode_lst = former_nodes_dict[vertex]
        sum_former_node_SH = sum([num_of_SH_dict[ele]
                                 for ele in formerNode_lst])
        for ele in formerNode_lst:
            # formerNode_num = len(former_nodes_dict[vertex])
            this_ele_SH_num = num_of_SH_dict[ele]
            this_vertex_value = value_dict[vertex]
            value_added = (this_vertex_value *
                           this_ele_SH_num / sum_former_node_SH)
            value_dict[ele] += value_added
            pair = tuple(sorted((ele, vertex)))
            pair_value_dict[pair] += value_added


# # Generate a BFS tree given a root node and then compute the edge betweenness
# def compute_edgeBetweenness(root, nodes, pair_value_dict, graph):
#     # dist: the shortest distances from source vertex
#     # initially all the elements are infinity except source vertex which is equal to 0
#     dist_dict = dict.fromkeys(nodes, 0)
#     # dist_dict[root] = 0
#     # print(dist_dict)

#     # the number of different shortest paths from the source vertex to this vertex
#     # all the elements are 0 except source vertex which is equal to 1
#     # num_of_SH_dict = dict.fromkeys(nodes, 0)
#     # num_of_SH_dict[root] = 1

#     # for each vertex, its former node(s) in the shortest path
#     former_nodes_dict = defaultdict(list)

#     # for each node, whether it is visited or not
#     visited = set()

#     # initiating value of each node is 1
#     value_dict = dict.fromkeys(nodes, 1)

#     # Root Node
#     q = []
#     q.append(root)
#     visited.add(root)
#     former_nodes_dict[root] = []

#     while q:
#         curr = q.pop(0)

#         # For all neighbors of current vertex do:
#         for node in graph[curr]:

#             # if the current vertex is not yet
#             # visited, then push it to the queue.
#             if node not in visited:
#                 q.append(node)
#                 visited.add(node)
#                 former_nodes_dict[node].append(curr)
#                 dist_dict[node] = dist_dict[curr] + 1
#             else:
#                 # neighbor_node = visited_dict[node]
#                 if dist_dict[node] > dist_dict[curr]:
#                     former_nodes_dict[node].append(curr)
#                 # # check if there is a better path.
#                 # if dist_dict[node] > dist_dict[curr] + 1:
#                 #     dist_dict[node] = dist_dict[curr] + 1
#                 #     # num_of_SH_dict[node] = num_of_SH_dict[curr]
#                 #     former_nodes_dict[node].append(curr)

#                 # # additional shortest paths found
#                 # elif dist_dict[node] == dist_dict[curr] + 1:
#                 #     # num_of_SH_dict[node] += num_of_SH_dict[curr]
#                 #     former_nodes_dict[node].append(curr)
#     dist_dict = {k: v for k, v in dist_dict.items() if k in visited}
# #   print(dist_dict)
# #   print(former_nodes_dict)
#     # First do some operations on nodes of dist 3, then dist 2, ....
#     # 一层一层往上遍历
#     # Scheme example: {3: ['A', 'C'], 2: ['B', 'G'], 1: ['D', 'F'], 0: ['E']}
#     groupByDist_dict = defaultdict(list)
#     for x, y in dist_dict.items():
#         groupByDist_dict[y].append(x)

#   return groupByDist_dict

#     traverse_list = flatten(list(collections.OrderedDict(
#         sorted(groupByDist_dict.items(), reverse=True)).values()))
#     # print(value_dict)
#     # print(former_nodes_dict)

#     for vertex in traverse_list[:-1]:
#         # print(vertex)
#         # update the value of its former node
#         formerNode_lst = former_nodes_dict[vertex]
#         for ele in formerNode_lst:
#             value_dict[ele] += (value_dict[vertex] /
#                                 len(former_nodes_dict[vertex]))
#             # print(value_dict)
#             pair = tuple(sorted((ele, vertex)))
#             # print(pair)
#             pair_value_dict[pair] += (value_dict[vertex] /
#                                       len(former_nodes_dict[vertex]))
#             # print(pair_value_dict)

# Sum the total edge betweeness


def compute_total_edgeBetweenness(graph_dict):
    # Get all the nodes of this graph
    Nodes = sorted(graph_dict.keys())
    # Get all the possible pairs of this graph
    # !!! Maybe time consuming
    pair_list = list(itertools.combinations(list(Nodes), 2))
    pair_value_dict = dict.fromkeys(pair_list, 0)
    for node in Nodes:
        compute_edgeBetweenness(node, Nodes, pair_value_dict, graph_dict)

    result_dict = {k: v / 2 for k, v in pair_value_dict.items() if v > 0}

    return sorted(result_dict.items(), key=lambda x: (-x[1], x[0][0]))


# Given a pair, return the number of common businesses
def num_common_busi(pair):
    set1 = uid_bid_dict[pair[0]]
    set2 = uid_bid_dict[pair[1]]
    return len(set1.intersection(set2))

# Return a list of nodes of one community


def bfs(visited, graph, node):  # function for BFS
    queue = []
    community = []
    visited.add(node)
    queue.append(node)

    while queue:          # Creating loop to visit each node
        m = queue.pop(0)
        community.append(m)

    for neighbour in graph[m]:
        if neighbour not in visited:
            visited.add(neighbour)
            queue.append(neighbour)

    return community


# Given a graph dict, output its community list
def find_community(graph):
    all_nodes = set(graph.keys())
    visited = set()

    # nodes that has not been visited:
    need_visited = all_nodes.difference(visited)

    # list of list of communities
    # Scheme: [community_list_1, community_list_2, ....]
    community = list()

    # Initialize a queue
    queue = []

    while len(visited) != len(all_nodes):
        random_node = need_visited.pop()
        this_community = sorted(bfs(visited, graph, random_node))
        need_visited = all_nodes.difference(visited)
        community.append(this_community)

    return community

# Given a list of communities, output total modularity


def compute_modularity(original_graph, after_graph):
    # M represents the edge number of the original graph
    M = 0
    for k, v in original_graph.items():
        M += len(v)
    M = M/2

    # Compute community
    community = find_community(after_graph)
    # print(community)

    total_modularity = 0
    for com in community:
        # print(com)
        for node_pair in itertools.combinations(com, 2):
            # print(node_pair)
            k_i = len(after_graph[node_pair[0]])
            k_j = len(after_graph[node_pair[1]])
            A = 1 if node_pair[1] in original_graph[node_pair[0]] else 0
            total_modularity += float(A - (k_i * k_j / (2 * M)))

    if M != 0:
        return total_modularity / (2 * M)
    else:
        return 0

# Given a graph dict,
# Return a graph dict after remove those edges with biggest edge betweenness


def find_graph_after_remove(graph):
    lst = compute_total_edgeBetweenness(graph)

    # print(lst)

    removal_edges = []

    if (len(lst) > 0):
        # The first line is the max value
        max_value = lst[0][1]
        removal_edges.append(lst.pop(0)[0])

        # Find if other lines has the same max value
        while len(lst) > 0 and lst[0][1] == max_value:
            removal_edges.append(lst.pop(0)[0])

    # Copy the original graph and based on this to have an after graph
    after_graph = copy.deepcopy(graph)

    for edge in removal_edges:
        after_graph[edge[0]].remove(edge[1])
        after_graph[edge[1]].remove(edge[0])
    return after_graph


def find_community_with_largest_modularity(graph):

    # First compute modularity for original graph, i.e., a graph without any removal
    modularity = compute_modularity(graph, graph)
    max = modularity
    # print(max)
    max_graph = graph
    # print(max_graph)

    while modularity > 0:
        after_graph = find_graph_after_remove(graph)
        modularity = compute_modularity(graph, after_graph)
        if modularity > max:
            max = modularity
            # print(max)
            max_graph = after_graph
            # print(max_graph)
        graph = after_graph

    result_list = find_community(max_graph)

    return sorted(result_list, key=lambda x: (len(x), x[0][0]))


configuration = SparkConf()
configuration.set("spark.driver.memory", "4g")
configuration.set("spark.executor.memory", "4g")
sc = SparkContext.getOrCreate(configuration)


filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
betweenness_output_file_path = sys.argv[3]
community_output_file_path = sys.argv[4]

# Schema: ('user_id', 'business_id')
rdd = sc.textFile(input_file_path).map(lambda line: line.split(','))

header = rdd.first()

# Schema: ('user_id': {'business_id1', 'business_id2', 'business_id3', ...})
# i.e. key: user_id value: set of its corresponding business_id
uid_bid_dict = rdd.filter(lambda row: row != header).groupByKey(
).mapValues(lambda b_ids: set(list(b_ids))).collectAsMap()

# Schema: [('user_id1', 'user_id2'), ('user_id1', 'user_id3')......
# i.e. user_id pairs
uid_pairs = list(itertools.combinations(list(uid_bid_dict.keys()), 2))

# Get all edges in a list
# Schema: [edge1, edge2, edge3, ...]
edge_lst = list()

for pair in uid_pairs:
    if num_common_busi(pair) >= filter_threshold:
        # If A-B is an edge, We include both (A, B) and (B, A) in this list
        edge_lst.append(tuple(pair))
        edge_lst.append(tuple((pair[1], pair[0])))

# Schema: {vertex: [all other vertices connect with this vertex]}
v_allOtherV_dict = sc.parallelize(edge_lst).groupByKey().mapValues(
    lambda b_ids: sorted(list(set(b_ids)))).collectAsMap()

result1 = compute_total_edgeBetweenness(v_allOtherV_dict)

with open(betweenness_output_file_path, 'w') as f:
    for ele in result1:
        f.write(f"{str(ele[0]) + ',' + str(round(ele[1], 5))}\n")


result2 = find_community_with_largest_modularity(v_allOtherV_dict)

with open(community_output_file_path, 'w') as f:
    for ele in result2:
        f.write(f"{str(ele)[1:-1]}\n")
