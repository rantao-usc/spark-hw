import sys
import time
import csv
import math
import numpy as np
from pyspark import SparkContext, SparkConf
import json
from operator import add
import pandas as pd
import xgboost as xgb
from sklearn import preprocessing
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error
import pickle
import os

configuration = SparkConf()
configuration.set("spark.driver.memory", "4g")
configuration.set("spark.executor.memory", "4g")
configuration.set("spark.driver.maxResultSize", "4g")
sc = SparkContext.getOrCreate(configuration)

folder_path = sys.argv[1]
test_file_name = sys.argv[2]
output_file_name = sys.argv[3]

TrainingDataSource = folder_path + "yelp_train.csv"
BusinessExtraDataSource = folder_path + "business.json"
PhotoExtraDataSource = folder_path + "photo.json"
ReviewTrainingDataSource = folder_path + "review_train.json"
UserExtraDataSource = folder_path + "user.json"

# Schema: ('user_id', 'business_id', 'stars')
rdd = sc.textFile(TrainingDataSource).map(lambda line: (line.split(',')[0], line.split(',')[1], line.split(',')[2]))
header = rdd.first()
original_rdd = rdd.filter(lambda row: row != header).map(lambda row: (row[0], row[1], float(row[2]))).distinct()

original_list = original_rdd.collect()

# Schema: ('user_id', 'business_id', 'stars')
rdd_test = sc.textFile(test_file_name).map(lambda line: (line.split(',')[0], line.split(',')[1], line.split(',')[2]))
header_test = rdd_test.first()
test_original_rdd = rdd_test.filter(lambda row: row != header).map(lambda row: (row[0], row[1], float(row[2]))).distinct()
test_original_list = test_original_rdd.collect()

# some extra data
businessInfo_rdd = sc.textFile(BusinessExtraDataSource).map(lambda row: json.loads(row)) \
                .map(lambda row: (row['business_id'], row['review_count'], row['stars']))

#b_hhhh = sc.textFile(BusinessExtraDataSource).map(lambda row: json.loads(row)).map(lambda row: row if row["attributes"] == None else 0).filter(lambda x: x != 0)

photo_rdd = sc.textFile(PhotoExtraDataSource).map(lambda row: json.loads(row)) \
                .map(lambda row: (row['business_id'], row['photo_id']))

review_train_rdd = sc.textFile(ReviewTrainingDataSource).map(lambda row: json.loads(row)) \
                .map(lambda row: (row['user_id'], row["business_id"], row['stars'], row['useful'], row['funny'], row['cool']))

userInfo_rdd = sc.textFile(UserExtraDataSource).map(lambda row: json.loads(row)).map(lambda row: (row["user_id"], row["review_count"], row["yelping_since"], row["friends"], row["useful"],
                                                                                                  row["funny"], row["cool"], row['fans'], row["average_stars"], row["compliment_hot"],
                                                                                                  row["compliment_more"],row["compliment_profile"],row["compliment_cute"],row["compliment_list"],
                                                                                                  row["compliment_note"],row["compliment_plain"],row["compliment_cool"],row["compliment_funny"],
                                                                                                  row["compliment_writer"],row["compliment_photos"]))

# Num of photos each business has
photoNumByBusiness_dict = photo_rdd.groupByKey().mapValues(len).collectAsMap()

# Num of reviews each business received
reviewNumByBusiness_dict = businessInfo_rdd.map(lambda row: (row[0], row[1])).collectAsMap()

# Average Stars of each business
averageStarByBusiness_dict = businessInfo_rdd.map(lambda row: (row[0], row[2])).collectAsMap()

# Variance of Stars of each business
varianceStarByBusiness_dict = review_train_rdd.map(lambda row: (row[1], row[2])).groupByKey().mapValues(list).mapValues(lambda x: np.var(x)).collectAsMap()

# Num of fans each user have
fansNumByUser_dict = userInfo_rdd.map(lambda row: (row[0], row[7])).collectAsMap()

# Num of friends each user have
friendsNumByUser_dict = userInfo_rdd.map(lambda row: (row[0], len(row[3])) if (row[3] != 'None') else (row[0], 0)).collectAsMap()

# Num of reviews each user write
reviewsNumByUser_dict = userInfo_rdd.map(lambda row: (row[0], row[1])).collectAsMap()

# Average stars of each user
averageStarByUser_dict = userInfo_rdd.map(lambda row: (row[0], row[8])).collectAsMap()

df_left = pd.DataFrame({"u_id": [row[0] for row in original_list], "b_id": [row[1] for row in original_list], "score": [row[2] for row in original_list]})

# Merge photo num
df_right = pd.DataFrame({"b_id": photoNumByBusiness_dict.keys(), "photoNum": photoNumByBusiness_dict.values()})
df_left = df_left.merge(df_right, how='left', on='b_id').fillna(0)

# Merge review num by business
df_right = pd.DataFrame({"b_id": reviewNumByBusiness_dict.keys(), "bus_reviewNum": reviewNumByBusiness_dict.values()})
df_left = df_left.merge(df_right, how='left', on='b_id').fillna(0)

# Merge average star by business
df_right = pd.DataFrame({"b_id": averageStarByBusiness_dict.keys(), "bus_star_avg": averageStarByBusiness_dict.values()})
df_left = df_left.merge(df_right, how='left', on='b_id').fillna(3)

# Merge variance star by business
df_right = pd.DataFrame({"b_id": varianceStarByBusiness_dict.keys(), "bus_star_var": varianceStarByBusiness_dict.values()})
df_left = df_left.merge(df_right, how='left', on='b_id').fillna(0)

# Merge num of fans by user
df_right = pd.DataFrame({"u_id": fansNumByUser_dict.keys(), "num_fans": fansNumByUser_dict.values()})
df_left = df_left.merge(df_right, how='left', on='u_id').fillna(0)

# Merge num of friends by user
df_right = pd.DataFrame({"u_id": friendsNumByUser_dict.keys(), "num_friends": friendsNumByUser_dict.values()})
df_left = df_left.merge(df_right, how='left', on='u_id').fillna(0)

# Merge num of reviews by user
df_right = pd.DataFrame({"u_id": reviewsNumByUser_dict.keys(), "user_reviewNum": reviewsNumByUser_dict.values()})
df_left = df_left.merge(df_right, how='left', on='u_id').fillna(0)

# Merge average star by user
df_right = pd.DataFrame({"u_id": averageStarByUser_dict.keys(), "user_star_avg": averageStarByUser_dict.values()})
df_left = df_left.merge(df_right, how='left', on='u_id').fillna(0)

df_left = df_left.drop(['b_id', 'u_id'], axis=1)

X = df_left.drop(['score'], axis=1).values
y = df_left['score'].values

scaler = preprocessing.StandardScaler()
scaler.fit(X)
X_scaled = scaler.transform(X)

model = xgb.XGBRegressor()
ml = xgb.XGBRegressor(colsample_bytree=0.4, learning_rate=0.1, \
                              max_depth=8, alpha=0, n_estimators=80, subsample=0.6, random_state=0)
ml.fit(X_scaled, y)

df_left = pd.DataFrame({"u_id": [row[0] for row in test_original_list], "b_id": [row[1] for row in test_original_list], "score": [row[2] for row in test_original_list]})

# Merge photo num
df_right = pd.DataFrame({"b_id": photoNumByBusiness_dict.keys(), "photoNum": photoNumByBusiness_dict.values()})
df_left = df_left.merge(df_right, how='left', on='b_id').fillna(0)

# Merge review num by business
df_right = pd.DataFrame({"b_id": reviewNumByBusiness_dict.keys(), "bus_reviewNum": reviewNumByBusiness_dict.values()})
df_left = df_left.merge(df_right, how='left', on='b_id').fillna(0)

# Merge average star by business
df_right = pd.DataFrame({"b_id": averageStarByBusiness_dict.keys(), "bus_star_avg": averageStarByBusiness_dict.values()})
df_left = df_left.merge(df_right, how='left', on='b_id').fillna(3)

# Merge variance star by business
df_right = pd.DataFrame({"b_id": varianceStarByBusiness_dict.keys(), "bus_star_var": varianceStarByBusiness_dict.values()})
df_left = df_left.merge(df_right, how='left', on='b_id').fillna(0)

# Merge num of fans by user
df_right = pd.DataFrame({"u_id": fansNumByUser_dict.keys(), "num_fans": fansNumByUser_dict.values()})
df_left = df_left.merge(df_right, how='left', on='u_id').fillna(0)

# Merge num of friends by user
df_right = pd.DataFrame({"u_id": friendsNumByUser_dict.keys(), "num_friends": friendsNumByUser_dict.values()})
df_left = df_left.merge(df_right, how='left', on='u_id').fillna(0)

# Merge num of reviews by user
df_right = pd.DataFrame({"u_id": reviewsNumByUser_dict.keys(), "user_reviewNum": reviewsNumByUser_dict.values()})
df_left = df_left.merge(df_right, how='left', on='u_id').fillna(0)

# Merge average star by user
df_right = pd.DataFrame({"u_id": averageStarByUser_dict.keys(), "user_star_avg": averageStarByUser_dict.values()})
df_left = df_left.merge(df_right, how='left', on='u_id').fillna(0)

df_left = df_left.drop(['b_id', 'u_id'], axis=1)

X_test = df_left.drop(['score'], axis=1).values
y_test = df_left['score'].values

X_test_scaled = scaler.transform(X_test)
y_pred = ml.predict(X_test_scaled)

df_ml = pd.DataFrame({"user_id": [row[0] for row in test_original_list], "business_id": [row[1] for row in test_original_list], "prediction": y_pred})


def generate_business_index_list(lst):
  # lst = list(set(list(lst)))
  return [busi_dict[ele] for ele in lst]

def generate_user_index_list(lst):
  return [user_dict[ele] for ele in lst]


# given two list of integer values
# e.g.
# vec_a = [1, 2, 3, 4, 5]
# vec_b = [1, 3, 5, 7, 9]
# return the Pearson similarity
def Pearson_similarity(vec_a, vec_b):
    # Dot and norm
    # assert len(vec_a) > 0, "len should greater than zero"
    # assert len(vec_b) > 0, "len should greater than zero"

    avg_a = sum(vec_a) / len(vec_a)
    avg_b = sum(vec_b) / len(vec_b)
    vec_a = [ele - avg_a for ele in vec_a]
    vec_b = [ele - avg_b for ele in vec_b]
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = sum(a * a for a in vec_a) ** 0.5
    norm_b = sum(b * b for b in vec_b) ** 0.5

    # Cosine similarity
    if norm_a * norm_b == 0:
        return 0

    cos_sim = dot / (norm_a * norm_b)

    return cos_sim


def computeSimilarity(dict1, dict2):
    co_rated_user = list(set(dict1.keys()) & (set(dict2.keys())))

    if len(co_rated_user) < 3:
        return 0

    vec_a = [int(float(dict1[user_id])) for user_id in co_rated_user]
    vec_b = [int(float(dict2[user_id])) for user_id in co_rated_user]

    return Pearson_similarity(vec_a, vec_b)

# Schema: ('user_id', 'business_id', 'stars')
rdd = sc.textFile(TrainingDataSource).map(lambda line: (line.split(',')[0], line.split(',')[1], line.split(',')[2]))
header = rdd.first()
original_rdd = rdd.filter(lambda row: row != header).map(lambda row: (row[0], row[1], float(row[2]))).distinct()

# Generate user dictionary
# Schema: {user_id: its_index}
user_dict = original_rdd.map(lambda row: row[0]).distinct().zipWithIndex().collectAsMap()

# Generate business dictionary
# Schema: {business_id: its_index}
busi_dict = original_rdd.map(lambda row: row[1]).distinct().zipWithIndex().collectAsMap()

# Group by key (user_id) and represent each business with its index in business list
# Schema example: (1, [1, 1846, 2954, 1113, 2655, 1658, 10357, 7311, 5865, 6933, 5970])
uindexTobindex_matrix_rdd = original_rdd.map(lambda row: (user_dict[row[0]], row[1])).groupByKey().mapValues(generate_business_index_list)

# Get a dictionary of this rdd, so that in the fature, we can get the b_id list of a certain u_id quickly
# 而这个b_id list成为了所有需要计算wi,j的list
uindexTobindex_dic = uindexTobindex_matrix_rdd.collectAsMap()

# group original data by bidx, convert the result to a dict
# key: bid
# value: a dict of dict {uid: score}
# 957/fThrN4tfupIGetkrz18JOg: {969: '4.0', 9481: '5.0', 2322: '4.0',....}
bindex_uindex_score_dict = original_rdd.map(lambda row: (busi_dict[row[1]], (user_dict[row[0]], row[2]))).groupByKey().mapValues(dict).collectAsMap()
bid_uindex_score_dict = original_rdd.map(lambda row: (row[1], (user_dict[row[0]], row[2]))).groupByKey().mapValues(dict).collectAsMap()

# Schema: {(uindex, bindex): score}
pair_score_dict = original_rdd.map(lambda row: ((user_dict[row[0]], busi_dict[row[1]]), row[2])).collectAsMap()

test_rdd = sc.textFile(test_file_name).map(lambda line: (line.split(',')[0], line.split(',')[1], line.split(',')[2]))

test_header = test_rdd.first()

test_original_rdd = test_rdd.filter(lambda row: row != test_header).distinct()


def getValue(u_index, a_tuple):
    lst = []

    for b_index, wij in a_tuple:
        lst.append((pair_score_dict[u_index, b_index], wij))

    return lst

def computeSimi(a_tuple_list):

    above = []
    below = []

    for score, wij in a_tuple_list:
        above.append(score * wij)
        below.append(abs(wij))

    sum_above = sum(above)
    sum_below = sum(below)

    if sum_below == 0:
        return 3

    return sum_above/sum_below


def computeS(u_id, b_id):
    # 如果是之前从来没有遇到过的user，直接return 3
    if u_id not in user_dict.keys():
        return 3
    if b_id not in busi_dict.keys():
        return 3

    b_index_lst = uindexTobindex_dic[user_dict[u_id]]
    sorted_list = wij(b_id, b_index_lst)
    value_lst = getValue(user_dict[u_id], sorted_list)

    return 0.1 * computeSimi(value_lst) + 0.5 * bid_avg_dict[b_id] + 0.4 * uid_avg_dict[u_id]


def wij(b_id, b_index_lst):
    if len(b_index_lst) == 0:
        simi = 0
    lst = []
    dict1 = bid_uindex_score_dict[b_id]

    for b_index in b_index_lst:
        dict2 = bindex_uindex_score_dict[b_index]
        simi = computeSimilarity(dict1, dict2)
        if simi != 0:
            lst.append((b_index, simi))
    sorted_list = sorted(lst, key=lambda x: -x[1])
    if len(sorted_list) > 5:
        return sorted_list[:5]
    else:
        return sorted_list

# avg star for each restaurant
bid_avg_dict = original_rdd.map(lambda row: (row[1], row[2])).groupByKey().map(lambda row: (row[0], sum(list(row[1]))/len(list(row[1])))).collectAsMap()
# avg star for each user
uid_avg_dict = original_rdd.map(lambda row: (row[0], row[2])).groupByKey().map(lambda row: (row[0], sum(list(row[1])) / len(list(row[1])))).collectAsMap()

result = test_original_rdd.map(lambda row: (row[0], row[1], computeS(row[0], row[1]))).collect()

df_1 = pd.DataFrame({"user_id":[row[0] for row in result], "business_id":[row[1] for row in result], "prediction":[row[2] for row in result]})


df_2 = df_ml

df = df_1.merge(df_2, on=["user_id", "business_id"])

df['final_prediction'] = 0.1*df['prediction_x'] + 0.9*df['prediction_y']

df_result = df[["user_id", "business_id", "final_prediction"]]

df_result.to_csv(output_file_name,index=False)