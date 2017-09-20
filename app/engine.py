# -*- coding: utf-8 -*-
import itertools
import json
import shutil
from contextlib import contextmanager
from math import sqrt
from operator import add

import math

import time
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import SparkSession

from multiprocessing import cpu_count

from distutils.dir_util import copy_tree



@contextmanager
def spark_context():
    conf = SparkConf().setMaster("local").setAppName("sample-recommender").set(
        "spark.executor.memory", "2g")
    context = SparkContext(conf=conf)
    yield context
    context.stop()


class RecommendationEngine(object):
    def __init__(self, source_name, rank=None, reg_param=None, max_iter=None, rmse=float('inf'), recommendation_count=5):
        self.source_name = source_name
        self.model_path = "models/als_{}".format(source_name)
        self.als_params = {
            'rank': rank,
            'reg_param': reg_param,
            'max_iter': max_iter,
            'rmse': rmse
        }
        self.partitions = cpu_count()
        self.recommendation_count = recommendation_count

    def import_model(self, model_path):
        spark = SparkSession.builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .master("local") \
            .getOrCreate()

        # copy the model to an internal path
        tmp_path = '{}_tmp'.format(self.model_path)
        try:
            copy_tree(model_path, tmp_path)
        except Exception:
            raise()

        try:
            ALSModel.load(tmp_path)
        except Exception:
            raise()

        try:
            shutil.rmtree(self.model_path)
            copy_tree(tmp_path, self.model_path)
            shutil.rmtree(tmp_path)
        except Exception:
            raise()

        return ALSModel.load(self.model_path)

    def _load_model(self):
        with open('{}/{}'.format(self.model_path, 'params.json')) as params_file:
            self.als_params = json.load(params_file)
        return ALSModel.load(self.model_path)

    def _persist_model(self, model):
        model.write().overwrite().save(self.model_path)
        with open('{}/{}'.format(self.model_path, 'params.json'), 'w') as params_file:
            json.dump(self.als_params, params_file)

    def train_new_model(self, training_data_file, validation_data_file, test_data_file, user_col, item_col, rating_col, **als_params):

        spark = SparkSession.builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .master("local") \
            .getOrCreate()

        training_data = spark.read.json(training_data_file)

        validation_data = spark.read.json(validation_data_file)

        test_data = spark.read.json(test_data_file)

        current_model = None
        self.als_params['rmse'] = float('inf')

        for rank, reg_param, max_iter in itertools.product(als_params['rank_opts'], als_params['reg_param_opts'], als_params['max_iter_opts']):

            als = ALS(rank=rank, regParam=reg_param, maxIter=max_iter, userCol=user_col, itemCol=item_col, ratingCol=rating_col)

            current_model = als.fit(training_data)

            rmse = self._compute_rmse(current_model, validation_data)

            if not math.isnan(rmse) and rmse < self.als_params['rmse']:
                self.als_params = {
                    'rank': rank,
                    'reg_param': reg_param,
                    'max_iter': max_iter,
                    'rmse': rmse
                }

        self.als_params['rmse'] = self._compute_rmse(current_model, test_data)

        self._persist_model(current_model)

        return self.als_params

    def train_existing_model(self, training_data_file, user_col, item_col, rating_col):
        spark = SparkSession.builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .master("local") \
            .getOrCreate()

        training_data = spark.read.json(training_data_file)

        self._load_model() # for loading the als_params

        new_model = ALS(rank=self.als_params['rank'], regParam=self.als_params['reg_param'], maxIter=self.als_params['max_iter'], userCol=user_col, itemCol=item_col, ratingCol=rating_col).fit(training_data)

        self._persist_model(new_model)

    def recommend_curated(self, user_id, product_catalog_file, products_used):
        spark = SparkSession.builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .master("local") \
            .getOrCreate()

        df = spark.read.json(product_catalog_file)

        df.createOrReplaceTempView("product_catalog")

        candidates = spark.sql("SELECT {} as user_id, product_id FROM product_catalog WHERE product_id not in {}".format(user_id, tuple(products_used)))

        model = self._load_model()

        predictions = model.transform(candidates)

        # remove all NaN values
        predictions = predictions.na.drop(subset=["prediction"])

        rows = predictions.orderBy('prediction', ascending=False).take(self.recommendation_count)

        return [i.product_id for i in rows]

    def recommend_default(self, training_data_file):
        spark = SparkSession.builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

        df = spark.read.json(training_data_file) # actually this should be entire data not just training data

        df.createOrReplaceTempView("user_ratings")

        candidates = spark.sql("SELECT product_id, sum(rating) as sum_ratings from user_ratings group by product_id")

        rows = candidates.orderBy('sum_ratings', ascending=False).take(self.recommendation_count)

        return [i.product_id for i in rows]

    @staticmethod
    def _compute_rmse(model, data):
        predictions = model.transform(data)
        # remove all NaN values
        predictions = predictions.na.drop(subset=["prediction"])
        evaluator = RegressionEvaluator(metricName="rmse",
                                        labelCol="rating",
                                        predictionCol="prediction")
        rmse = evaluator.evaluate(predictions)
        return rmse




