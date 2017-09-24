# -*- coding: utf-8 -*-
import itertools
import json
import logging
import math
import os
from abc import ABC, abstractmethod

from py4j.protocol import Py4JJavaError
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from core import config, utils
from core.warehouse import FileWarehouse

logger = logging.getLogger(__name__)


class RecommendationEngine(ABC):
    @abstractmethod
    def train_new_model(self, **als_opts):
        pass

    @abstractmethod
    def export(self, path):
        pass

    @classmethod
    @abstractmethod
    def import_from_path(cls, path):
        pass

    @abstractmethod
    def retrain_with_updated_data(self):
        pass

    @abstractmethod
    def generate_recommendations(self):
        pass


class ALSRecommendationEngine(RecommendationEngine):
    def __init__(self, warehouse: FileWarehouse, recommendation_count=5,
                 als_params=None, model=None):

        self.warehouse = warehouse

        self.recommendation_count = recommendation_count

        self.als_params = {} if als_params is None else als_params

        self.model = model

        self._load_spark_session()

    @classmethod
    def _load_spark_session(cls):
        cls.spark = SparkSession.builder \
            .appName("ALS Recommendation Engine") \
            .master("local") \
            .getOrCreate()

    def ready(self) -> bool:
        ready = self.als_params != {} and self.model is not None

        if not ready:
            logger.warning('engine is not ready.')

        return ready

    def export(self, path):
        utils.create_directory(path)

        if self.ready():
            self._persist_model(path=path, model=self.model)

        self._persist_params(path=path,
                             warehouse_partition=self.warehouse.partition,
                             recommendation_count=self.recommendation_count,
                             als_params=self.als_params)

    @classmethod
    def import_from_path(cls, path):
        cls._load_spark_session()

        model = cls._load_model(path)

        params = cls._load_params(path)

        engine = cls(
            warehouse=FileWarehouse(partition=params['warehouse_partition']),
            recommendation_count=params['recommendation_count'],
            als_params=params['als_params'],
            model=model)

        return engine

    def train_new_model(self, **als_opts) -> dict:
        logger.info('starting training of a new model...')

        training_data = self.spark.read.json(self.warehouse.training_file)

        validation_data = self.spark.read.json(self.warehouse.validation_file)

        test_data = self.spark.read.json(self.warehouse.test_file)

        current_model = None

        current_als_params = {
            'rmse': float('inf')
        }

        for rank, reg_param, max_iter in itertools.product(
                als_opts['rank_opts'], als_opts['reg_param_opts'],
                als_opts['max_iter_opts']):

            logger.debug('training model for rank: {}, reg_param: {}, max_iter:'
                         ' {}...'.format(rank, reg_param, max_iter))

            current_model = ALS(rank=rank,
                                regParam=reg_param,
                                maxIter=max_iter,
                                userCol=config.USER_COL,
                                itemCol=config.PRODUCT_COL,
                                ratingCol=config.RATINGS_COL) \
                .fit(training_data)

            current_rmse = self._compute_rmse(current_model, validation_data)

            logger.debug('rmse found:{}'.format(current_rmse))

            if not math.isnan(current_rmse) and current_rmse < \
                    current_als_params['rmse']:
                current_als_params = {
                    'rank': rank,
                    'reg_param': reg_param,
                    'max_iter': max_iter,
                    'rmse': current_rmse
                }

        # compute rmse on the test data
        current_als_params['rmse'] = self._compute_rmse(current_model,
                                                        test_data)

        logger.debug(
            'rmse on the test data: {}'.format(current_als_params['rmse']))

        self.model = current_model

        self.als_params = current_als_params

        logger.info(
            'model trained and saved successfully. params are: {}'.format(
                self.als_params))

        return self.als_params

    def retrain_with_updated_data(self) -> None:
        assert self.ready()

        logger.info('starting training of the current model...')

        training_data = self.spark.read.json(self.warehouse.training_file)

        self.model = ALS(rank=self.als_params['rank'],
                         regParam=self.als_params['reg_param'],
                         maxIter=self.als_params['max_iter'],
                         userCol=config.USER_COL,
                         itemCol=config.PRODUCT_COL,
                         ratingCol=config.RATINGS_COL) \
            .fit(training_data)

        logger.info('model trained successfully.')

    def generate_recommendations(self) -> None:
        logger.debug('starting the batch recommendation job...')

        default_recommendations = self._generate_default_recommendations()

        self.warehouse.update_recommendations(-1, default_recommendations)

        try:
            assert os.stat(self.warehouse.users_file).st_size > 0

            users = self.spark.read.json(self.warehouse.users_file).select(
                config.USER_COL).collect()

            for user in users:
                recommendations = \
                    self._generate_recommendations_for_user(user.user_id)
                self.warehouse.update_recommendations(user.user_id, recommendations)

        except AssertionError:
            logger.warning('the users file is empty. '
                           'Perhaps no users have rated anything yet.')

    def _generate_recommendations_for_user(self, user_id: int) -> list:

        assert self.ready()

        logger.info('generating curated recommendations'
                    ' for user id: {}'.format(user_id))

        df = self.spark.read.json(self.warehouse.products_file)

        df.createOrReplaceTempView("product_catalog")

        query = "SELECT {} as {}, {} FROM product_catalog"\
                .format(user_id, config.USER_COL, config.PRODUCT_COL)

        candidates = self.spark.sql(query)

        candidates.createOrReplaceTempView('candidates')

        logger.debug('candidate products loaded successfully.')

        predictions = self.model.transform(candidates)

        # remove all NaN values
        predictions = predictions.na.drop(subset=["prediction"])

        rows = predictions.orderBy('prediction', ascending=False).take(
            self.recommendation_count)

        recommendations = [i.product_id for i in rows]

        logger.info(
            'curated recommendations generated for user id {}: {}'
            .format(user_id, recommendations))

        return recommendations

    def _generate_default_recommendations(self) -> list:
        logger.info('generating the default recommendations...')

        df = self.spark.read.json(self.warehouse.ratings_file)

        df.createOrReplaceTempView("user_ratings")

        candidates = self.spark.sql(
            "SELECT product_id, sum(ratings) AS overall_ratings "
            "FROM user_ratings GROUP BY product_id")

        rows = candidates.orderBy('overall_ratings', ascending=False).take(
            self.recommendation_count)

        recommendations = [i.product_id for i in rows]

        logger.info('default recommendations generated.')

        return recommendations

    @staticmethod
    def _load_params(path) -> dict:
        with open('{}/{}'.format(path, 'params.json')) as params_file:
            return json.load(params_file)

    @staticmethod
    def _load_model(path) -> ALSModel:
        try:
            return ALSModel.load(path)
        except (Py4JJavaError, AnalysisException):
            logger.warning('no model found at path {}'.format(path))

    @staticmethod
    def _persist_model(path: str, model: ALSModel) -> None:
        model.write().overwrite().save(path)

    @staticmethod
    def _persist_params(path: str, warehouse_partition, recommendation_count,
                        als_params) -> None:
        params = {
            'warehouse_partition': warehouse_partition,
            'recommendation_count': recommendation_count,
            'als_params': als_params
        }

        with open('{}/{}'.format(path, 'params.json'), 'w') as params_file:
            json.dump(params, params_file)

    @staticmethod
    def _compute_rmse(model: ALSModel, data: DataFrame) -> float:
        predictions = model.transform(data)

        # remove all NaN values
        predictions = predictions.na.drop(subset=["prediction"])

        try:
            evaluator = RegressionEvaluator(metricName="rmse",
                                            labelCol=config.RATINGS_COL,
                                            predictionCol="prediction")
            rmse = evaluator.evaluate(predictions)
            return rmse
        except Exception as e:
            logger.warning(
                'Error in computing rmse. Error description: {}'.format(e))
            return math.nan
