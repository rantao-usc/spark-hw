# Yelp User Community Detection with Graph Analysis [[code](https://github.com/rantao-usc/spark-hw/blob/master/HW4/task2.py)]

This project focuses on the analysis of Yelp review data to create a user-centric graph. Leveraging the power of Apache Spark, we handle a massive dataset of 10 million user ratings to generate the graph and perform community detection. This allows us to identify similar users based on their review behavior.

## Graph Generation:

The graph is derived from the Yelp review dataset, where each node represents a user. An edge is established between two nodes if the count of businesses reviewed by both users meets or exceeds a pre-defined threshold. This helps in defining a connection based on mutual reviews.

For instance, considering user1 reviews [business1, business2, business3] and user2 reviews [business2, business3, business4, business5]. If the threshold is set to 2, an edge will be established between user1 and user2, indicating common interests.

Users with no connections (isolated nodes) are not included in the graph. In this project, we employed a filter threshold of 7 to generate significant connections.

## Community Detection and User Segmentation:

This project leverages the Girvan-Newman algorithm, implemented using Spark RDD without resorting to pre-built packages, to perform community detection within the user-centric graph.

Community detection plays a vital role in understanding user behavior and segmenting the user base for targeted marketing efforts. Users belonging to the same community showcase similar behaviors, offering valuable insights for business strategy.

## Technologies and Libraries:

- Python
- PySpark
- Yelp Review Data

## How to Run:

1. Ensure you have PySpark installed and properly configured.
2. Clone the repository and navigate to the relevant directory.
3. Run the Python script by using the following command:

```bash
spark-submit task2.py <input_file_path> <betweenness_output_file_path> <community_output_file_path>
```
Replace `<input_file_path>`, `<betweenness_output_file_path>`, and `<community_output_file_path>` with the paths to your input data and your desired output file paths.

## Contributions and License:

This is a student project and not open to contributions. However, feel free to explore, fork, and use the code under the MIT license. If you have any questions or suggestions, please open an issue.

## Contact:

If you have any questions or want to discuss this project further, please feel free to reach out.
