import sys
from pyspark import SparkContext, SparkConf
import json
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn import preprocessing
from sklearn.metrics import mean_squared_error
from pyspark.rdd import RDD
from typing import List, Dict, Tuple, Union


def initialize_spark() -> SparkContext:
    """
    Initialize and configure a Spark context.

    Returns:
       SparkContext: The configured Spark context.
    """
    configuration = SparkConf()
    configuration.set("spark.driver.memory", "4g")
    configuration.set("spark.executor.memory", "4g")
    configuration.set("spark.driver.maxResultSize", "4g")
    return SparkContext.getOrCreate(configuration)


def read_rdd_from_csv(path: str, header: bool = True) -> RDD:
    """
    Read an RDD from a CSV file and optionally skip header.

    Args:
        path (str): Path to the CSV file.
        header (bool): If True, skips the header row.

    Returns:
        RDD: The resulting RDD after reading the file.
    """
    rdd = sc.textFile(path).map(lambda line: (line.split(
        ',')[0], line.split(',')[1], float(line.split(',')[2])))
    if header:
        header_value = rdd.first()
        rdd = rdd.filter(lambda row: row != header_value)
    return rdd.distinct()


def parse_json_file(path: str, schema: List[str]) -> RDD:
    """
    Parse a JSON file into an RDD based on the provided schema.

    Args:
        path (str): Path to the JSON file.
        schema (list): List of fields to extract from the JSON.

    Returns:
        RDD: RDD containing tuples of the extracted data.
    """
    return sc.textFile(path).map(lambda row: json.loads(row)).map(lambda row: tuple(row[field] for field in schema))


def get_dicts_from_rdd(rdd: RDD, key_idx: int, value_idx: int) -> Dict[Union[str, float], Union[str, float]]:
    """
    Convert RDD into a dictionary based on specified columns.

    Args:
        rdd (RDD): Input RDD.
        key_idx (int): Index for the dictionary key.
        value_idx (int): Index for the dictionary value.

    Returns:
        dict: Dictionary constructed from the RDD.
    """
    return rdd.map(lambda row: (row[key_idx], row[value_idx])).collectAsMap()


def create_df_from_dicts(data_list: List[Tuple[str, str, float]], dicts: List[Dict[Union[str, float], Union[str, float]]], dict_labels: List[str]) -> pd.DataFrame:
    """
    Create a pandas DataFrame from dictionaries.

    Args:
        data_list (list): List of data.
        dicts (list): List of dictionaries to add to the DataFrame.
        dict_labels (list): List of column names corresponding to the dictionaries.

    Returns:
        DataFrame: Pandas DataFrame constructed from the input.
    """
    df = pd.DataFrame(data_list, columns=["u_id", "b_id", "score"])
    for idx, dict_item in enumerate(dicts):
        df_temp = pd.DataFrame(
            {"b_id": list(dict_item.keys()), dict_labels[idx]: list(dict_item.values())})
        df = df.merge(df_temp, how='left', on='b_id').fillna(0)
    return df.drop(['b_id', 'u_id'], axis=1)


def get_X_and_y_from_df(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """
    Extract features and labels from a DataFrame.

    Args:
        df (DataFrame): Input pandas DataFrame.

    Returns:
        tuple: Features array (X) and labels array (y).
    """
    X = df.drop(['score'], axis=1).values
    y = df['score'].values
    return X, y


def preprocess_data(X: np.ndarray) -> Tuple[np.ndarray, preprocessing.StandardScaler]:
    """
    Preprocess data using standard scaling.

    Args:
        X (array): Features array.

    Returns:
        tuple: Scaled features array and the scaler object.
    """
    scaler = preprocessing.StandardScaler()
    scaler.fit(X)
    return scaler.transform(X), scaler


def main():

    # Initiate Spark Environment
    global sc
    sc = initialize_spark()

    # Get value for variable from command line input
    folder_path = sys.argv[1]
    test_file_name = sys.argv[2]

    # Use a dictionary to store all the paths of data files
    data_files = {
        "train": folder_path + "yelp_train.csv",
        "business": folder_path + "business.json",
        "photo": folder_path + "photo.json",
        "review_train": folder_path + "review_train.json",
        "user": folder_path + "user.json"
    }

    # Read an RDD from a training CSV file and a testing CSV file
    train_rdd = read_rdd_from_csv(data_files["train"])
    test_rdd = read_rdd_from_csv(test_file_name, header=False)

    # Specify schema for each data file
    business_schema = ['business_id', 'review_count', 'stars']
    photo_schema = ['business_id', 'photo_id']
    review_train_schema = ['user_id', 'business_id', 'stars']
    user_schema = ['user_id', 'review_count', 'yelping_since',
                   'friends', 'useful', 'funny', 'cool', 'fans', 'average_stars']

    # Parse JSON files into corresponding RDDs based on the provided schema
    business_rdd = parse_json_file(data_files["business"], business_schema)
    photo_rdd = parse_json_file(data_files["photo"], photo_schema)
    review_train_rdd = parse_json_file(
        data_files["review_train"], review_train_schema)
    user_rdd = parse_json_file(data_files["user"], user_schema)

    # Convert RDDs into corresponding dictionaries based on specified columns
    photo_num_dict = get_dicts_from_rdd(photo_rdd, 0, 1)
    review_num_dict = get_dicts_from_rdd(business_rdd, 0, 1)
    average_star_dict = get_dicts_from_rdd(business_rdd, 0, 2)
    variance_star_dict = get_dicts_from_rdd(review_train_rdd, 1, 2)
    fans_num_dict = get_dicts_from_rdd(user_rdd, 0, 7)
    friends_num_dict = get_dicts_from_rdd(user_rdd, 0, 3)

    # Create a pandas DataFrame from dictionaries and then used for machine learning training process
    data_dicts = [photo_num_dict, review_num_dict, average_star_dict,
                  variance_star_dict, fans_num_dict, friends_num_dict]
    dict_labels = ["photoNum", "bus_reviewNum", "bus_star_avg",
                   "bus_star_var", "num_fans", "num_friends"]
    df_train = create_df_from_dicts(
        train_rdd.collect(), data_dicts, dict_labels)
    X_train, y_train = get_X_and_y_from_df(df_train)
    # Preprocess data using standard scaling
    X_train_scaled, scaler = preprocess_data(X_train)

    # Use XG-Boost to train the data
    ml = xgb.XGBRegressor(colsample_bytree=0.4, learning_rate=0.1, max_depth=8,
                          alpha=0, n_estimators=80, subsample=0.6, random_state=0)
    ml.fit(X_train_scaled, y_train)

    df_test = create_df_from_dicts(test_rdd.collect(), data_dicts, dict_labels)
    X_test, y_test = get_X_and_y_from_df(df_test)
    X_test_scaled = scaler.transform(X_test)

    y_pred = ml.predict(X_test_scaled)

    # Print the mean square error
    mse = mean_squared_error(y_test, y_pred)
    print(f"Mean Squared Error: {mse}")


if __name__ == "__main__":
    main()
