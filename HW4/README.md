# Generating a graph based on 10M user ratings for various businesses on Yelp, and performing community detection on the graph to identify similar users. [[code](https://github.com/rantao-usc/spark-hw/blob/master/HW4/task2.py)]

## Generating a graph:

Using the Yelp review dataset that contains user_id and business_id, with a massive 10 million user data, we can create a social network graph where each node represents a user. An edge between two nodes will exist if the number of times that two users review the same business is greater than or equal to the filter threshold. For example, if user1 reviewed [business1, business2, business3] and user2 reviewed [business2, business3, business4, business5], and the threshold is set to 2, then there will be an edge connecting user1 and user2.

If a user node has no edge connections, we will not include that node in the graph. For this assignment, we will use a filter threshold of 7.

## Identify similar users through user segmentation

Implement the Girvan-Newman algorithm using Spark RDD without calling existing packages to detect communities in the network graph. Identifying communities in the network graph can help with user segmentation, which is essential in marketing analysis. Users within the same communities are considered similar users.
