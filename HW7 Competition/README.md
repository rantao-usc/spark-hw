# Spark-XGBoost Data Processing and Prediction

## Overview

This script is designed to process and predict data using Apache Spark and the XGBoost machine learning framework. It initializes the Spark context, loads data from CSV and JSON files, performs data transformation, and finally predicts using an XGBoost model.

The link of the Phttps://github.com/rantao-usc/spark-hw/blob/master/HW7%20Competition/competition.py

## Illustrative Diagram
![Alternative Text](https://github.com/rantao-usc/spark-hw/blob/master/HW7%20Competition/demochart.drawio.png)

## Key Functionalities

1. **Initialization and Configuration**:

   - Spark context is initialized with a specific configuration.
   - Data is loaded from CSV and JSON files into RDDs.
   - JSON data is parsed based on a provided schema.

2. **Data Transformation and Feature Engineering**:

   - RDDs are converted into dictionaries based on specified columns.
   - A pandas DataFrame is created from these dictionaries for machine learning training.
   - Features and targets are extracted from the DataFrame.
   - Data is preprocessed using standard scaling.

3. **Machine Learning**:
   - XGBoost model is trained on the data.
   - Predictions are made on test data.
   - Mean squared error of the predictions is calculated and printed.

## Documentation
A documentation HTML file was created to document all the information of the functions and classes, as well as detailed explanations for each step.

![Alternative Text](https://github.com/rantao-usc/spark-hw/blob/45bd0f2797d26e02978ad3aba24f87e95f867992/HW7%20Competition/documentation.jpg)

The HTML file can be found at the link below：

https://github.com/rantao-usc/spark-hw/tree/master/HW7%20Competition/docs/build/html



## How to Run

1. Ensure you have the necessary dependencies installed:

   - pyspark
   - pandas
   - numpy
   - xgboost
   - sklearn

2. You can run the script using the command:

```
python competition.py /path/to/folder test_file.csv
```

3. Make sure that the necessary data files are in the expected directory or adjust the paths in the script accordingly.

## Note

Please be aware that this script assumes certain data structures and file formats. Adjustments may be needed based on the exact nature and format of your data.
