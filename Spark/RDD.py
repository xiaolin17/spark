from __future__ import print_function

import sys
import time
import re
from datetime import datetime
from pyspark import SparkContext
import numpy as np


def get_column(line, column_index1, column_index2):
    if 'userId' in line or 'movieId' in line:
        return (-1, -1)
    parts = line.split(',')
    return (int(parts[column_index1]), parts[column_index2])


def get_column2(line, column_index1):
    if 'userId' in line or 'movieId' in line:
        return (-1, -1)
    parts = line.split(',')
    return (int(parts[column_index1]), parts[-1])


def get_genres(line):
    if line == -1:
        return (-1, -1)
    parts = line.split('|')
    return (1, parts)


def filter_rating(line, column_index, threshold):
    if 'userId' in line or 'movieId' in line:
        return False
    parts = line.split(',')
    part = parts[column_index]
    if float(part) >= threshold:
        return True
    else:
        return False


def seq_op(r, rating):
    total_rating = r[0] + float(rating)
    count = r[1] + 1
    return (total_rating, count)


def comb_op(p1, p2):
    return (p1[0] + p2[0], p1[1] + p2[1])


def my_avg(x):
    (key, (total, count)) = x
    return (key, total / count)


def my_num(x):
    (key, (total, count)) = x
    return (key, count)


if __name__ == "__main__":
    sc = SparkContext(appName="PythonCombineByKey")

    # movies
    movie_id_index_movie = 0
    movie_title_index = 1
    movie_genres_index = 2
    movie_lines = sc.textFile(sys.argv[1], 1)
    movie_rdd = movie_lines.map(lambda x: get_column(x, movie_id_index_movie, movie_title_index))
    movie_count = movie_rdd.count()
    print("moviesCount: {}".format(movie_count - 1))

    movie_genres_rdd = movie_lines.map(lambda x: get_column2(x, movie_id_index_movie))
    movie_genres_rdd2 = movie_genres_rdd.map(lambda x: get_genres(x[1])).filter(lambda x: x[0] > 0).flatMapValues(
        lambda x: x)

    print("moviesTypeCount: {}".format(movie_genres_rdd2.distinct().count()))

    # ratingCount
    movie_id_index_rating = 1
    user_id_index_rating = 0
    rating_index = 2
    threshold = 4
    rating_lines = sc.textFile(sys.argv[2], 1)
    rating_rdd = rating_lines.map(lambda x: get_column(x, movie_id_index_rating, rating_index))  # movies rating
    rating_rdd2 = rating_lines.map(lambda x: get_column(x, user_id_index_rating, rating_index))  # usr rating
    rating_count = rating_rdd.count()

    print("ratingCount: {}".format(rating_count - 1))

    # movies rating>4
    agg_rating_rdd = rating_rdd.aggregateByKey((0, 0), seq_op, comb_op)
    agg_rating_rdd2 = rating_rdd2.aggregateByKey((0, 0), seq_op, comb_op)
    avg_rating_rdd = agg_rating_rdd.map(lambda x: my_avg(x))
    avg_rating_rdd_user = agg_rating_rdd2.map(lambda x: my_avg(x))
    number_rating_rdd = agg_rating_rdd.map(lambda x: my_num(x))
    filtered_rating_rdd = avg_rating_rdd.filter(lambda x: x[1] > threshold)
    filtered_rating_rdd1 = avg_rating_rdd.filter(lambda x: x[1] == 5)
    rating_5_count = filtered_rating_rdd1.count()
    rating_4_count = filtered_rating_rdd.count()

    print("movies rating=5: {}".format(rating_5_count - 1))

    print("movies rating>4: {}".format(rating_4_count - 1))

    print("movies avg rating: {}".format(avg_rating_rdd_user.collect()[:5]))

    print("move ratingCount: {}".format(number_rating_rdd.collect()[:5]))
