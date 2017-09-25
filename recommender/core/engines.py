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
from core.warehouse import FileWarehouse, Warehouse

logger = logging.getLogger(__name__)


class RecommendationEngine(ABC):
    """ Documents the APIs that the recommendation engine should expose.

    Multiple engines can be trained on the system. All a new engine has to
    do is implement the methods of this abstract engine.

    Attributes:
        warehouse: Any warehouse that implements the `Warehouse` contract.

        recommendation_count: The number of recommendations that the engine
        would generate.

        model: The ML model object that the engine uses.

        model_params: The parameters that describe the model.

    """

    def __init__(self, warehouse: Warehouse, recommendation_count: int, model,
                 model_params: dict):
        self.warehouse = warehouse

        self.recommendation_count = recommendation_count

        self.model = model

        self.model_params = {} if model_params is None else model_params

    @abstractmethod
    def train_new_model(self, **model_opts) -> dict:
        """ Trains a new model.

        Chooses the best combination from the options provided.
        The method calls the warehouse APIs internally to locate the data sets.

        Args:
            model_opts: Model-specific keyword arguments, each containing a
            tuple of values for the engine to choose from.

        Returns:
            A dict with the model parameters and their values.
        """
        pass

    @abstractmethod
    def export(self, path: str) -> None:
        """ Serializes and exports the engine to disk.

        Args:
            path: path on the disk where the engine should be exported to.

        """
        pass

    @classmethod
    @abstractmethod
    def import_from_path(cls, path: str) -> 'RecommendationEngine':
        """ Deserializes and creates a new engine instance from disk.

        Args:
            path: path on the disk where a previous engine was exported to.

        """
        pass

    @abstractmethod
    def retrain_with_updated_data(self) -> None:
        """ Retrains the engine on a new data set.

        This method requires a pre-trained model to be present. The model
        parameters are not mutated.
        The method calls the warehouse APIs internally to locate the data sets.

        """
        pass

    @abstractmethod
    def generate_recommendations_for_user(self, user_id: int) -> None:
        """ Generates recommendations for a user.

        This method requires a pre-trained model to be present.
        The method calls the warehouse APIs internally to locate the data sets.

        Args:
             user_id: the id of the user for whom recommendations are to be
             generated.
        """
        pass


class ALSRecommendationEngine(RecommendationEngine):
    """ A recommendation engine that uses the ALS (Alternating Least Squares)
    model provided by Apache Spark. For more details see
    https://spark.apache.org/docs/2.1.1/ml-collaborative-filtering.html .

    Implements the `RecommendationEngine` contract. For more details see
    `RecommendationEngine`.

    Attributes:
        same as `RecommendationEngine`.

    """

    def __init__(self, warehouse: FileWarehouse, recommendation_count: int = 5,
                 model_params: dict = None, model: ALSModel = None):
        """ Instantiates the engine and loads a spark session.

        Args:
            same as `RecommendationEngine`.

        """
        super().__init__(warehouse=warehouse,
                         recommendation_count=recommendation_count,
                         model=model,
                         model_params=model_params)

        self._load_spark_session()

    @classmethod
    def _load_spark_session(cls):
        """ Loads a spark session bound at the class level. """
        cls.spark = SparkSession.builder \
            .appName("ALS Recommendation Engine") \
            .master("local") \
            .getOrCreate()

    def ready(self) -> bool:
        """ A simple method to check if the engine is ready. An engine is
        considered ready if it has a pre-trained model present.

        Returns: True if engine is ready, false otherwise.

        """
        ready = self.model is not None

        if not ready:
            logger.warning('engine is not ready.')

        return ready

    def export(self, path: str) -> None:
        """ Implements the export method as defined in `RecommendationEngine`.

        Args:
            same as `RecommendationEngine.export`.

        """
        utils.create_directory(path)

        if self.ready():
            self._persist_model(path=path, model=self.model)

        self._persist_params(path=path,
                             warehouse_partition=self.warehouse.partition,
                             recommendation_count=self.recommendation_count,
                             model_params=self.model_params)

    @classmethod
    def import_from_path(cls, path: str) -> 'ALSRecommendationEngine':
        """ Implements the import method as defined in `RecommendationEngine`.

        Args:
            same as `RecommendationEngine.export`.

        Returns:
            a new `ALSRecommendationEngine` instance.

        """
        cls._load_spark_session()

        model = cls._load_model(path)

        params = cls._load_params(path)

        engine = cls(
            warehouse=FileWarehouse(partition=params['warehouse_partition']),
            recommendation_count=params['recommendation_count'],
            model_params=params['model_params'],
            model=model)

        return engine

    def train_new_model(self, **als_opts) -> dict:
        """ Implements the train method as defined in `RecommendationEngine`.

        Args:
            als_opts: The keyword arguments `rank`, `reg_param` and `max_iter`
            which define an ALS model. Used in the spirit as mentiond in
            `RecommendationEngine.train_new_model`.

        Returns:
            A dict with the chosen values of `rank`, `reg_param` and `max_iter`.

        """
        logger.info('starting training of a new model...')

        # load data sets
        training_data = self.spark.read.json(self.warehouse.training_file)
        validation_data = self.spark.read.json(self.warehouse.validation_file)
        test_data = self.spark.read.json(self.warehouse.test_file)

        # fix some (sane) defaults for the current untrained model.
        current_model = None
        current_model_params = {
            'rmse': float('inf')
        }

        # cycle through all possible combinations of the options provided.
        # choose the best combination (the one with the lowest RMSE).
        for rank, reg_param, max_iter in itertools.product(
                als_opts['rank_opts'],
                als_opts['reg_param_opts'],
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

            if not math.isnan(current_rmse) and \
                    current_rmse < current_model_params['rmse']:
                current_model_params = {
                    'rank': rank,
                    'reg_param': reg_param,
                    'max_iter': max_iter,
                    'rmse': current_rmse
                }

        # once the model is trained, compute the RMSE on test dataset.
        # this gives us an idea of the typical RMSE to expect from this model.
        current_model_params['rmse'] = self._compute_rmse(current_model,
                                                          test_data)
        logger.debug(
            'rmse on the test data: {}'.format(current_model_params['rmse']))

        # attach this model to the engine. It is ready now.
        self.model = current_model
        self.model_params = current_model_params
        logger.info(
            'model trained and ready. params are: {}'.format(self.model_params))

        return self.model_params

    def retrain_with_updated_data(self) -> None:
        """ Implements the retrain method as defined in `RecommendationEngine`.

        """
        assert self.ready()

        logger.info('starting training of the current model...')

        # load the updated data
        training_data = self.spark.read.json(self.warehouse.ratings_file)

        # train the existing model on the updated data
        self.model = ALS(rank=self.model_params['rank'],
                         regParam=self.model_params['reg_param'],
                         maxIter=self.model_params['max_iter'],
                         userCol=config.USER_COL,
                         itemCol=config.PRODUCT_COL,
                         ratingCol=config.RATINGS_COL) \
            .fit(training_data)

        logger.info('model trained successfully.')

    def generate_recommendations(self) -> None:
        """ Churns out the recommendations for all users in a batch fashion.

        A wrapper over the `generate_recommendations_for_user` and
        `generate_default_recommendations` methods.

        """
        logger.debug('starting the batch recommendation job...')

        # generate and store the default recommendations
        default_recommendations = self.generate_default_recommendations()
        self.warehouse.update_recommendations(config.DEFAULT_USERID,
                                              default_recommendations)

        # load users from the warehouse and generate recommendations for them
        # one by one.
        try:
            assert os.stat(self.warehouse.users_file).st_size > 0
            users = self.spark.read.json(self.warehouse.users_file).select(
                config.USER_COL).collect()
            for user in users:
                rec = self.generate_recommendations_for_user(user.user_id)
                self.warehouse.update_recommendations(user.user_id, rec)
        except AssertionError:
            logger.warning('the users file is empty. '
                           'Perhaps no users have rated anything yet.')

    def generate_recommendations_for_user(self, user_id: int) -> list:
        """ Implements generating recommendations for a given user as defined
        in `RecommendationEngine`.

        Args:
            same as in `RecommendationEngine.generate_recommendations_for_user`.

        Returns:
            same as in `RecommendationEngine.generate_recommendations_for_user`.
        """
        assert self.ready()

        logger.info('generating curated recommendations'
                    ' for user id: {}'.format(user_id))

        # load the products catalog to get all the candidate product ids
        df = self.spark.read.json(self.warehouse.products_file)
        df.createOrReplaceTempView("product_catalog")
        query = "SELECT {} as {}, {} FROM product_catalog" \
            .format(user_id, config.USER_COL, config.PRODUCT_COL)
        candidates = self.spark.sql(query)
        logger.debug('candidate products loaded successfully.')

        # generate recommendations and filter out invalid values like NaN.
        predictions = self.model.transform(candidates)
        predictions = predictions.na.drop(subset=["prediction"])
        rows = predictions.orderBy('prediction', ascending=False).take(
            self.recommendation_count)
        recommendations = [i.product_id for i in rows]

        logger.info(
            'curated recommendations generated for user id {}: {}'
                .format(user_id, recommendations))

        return recommendations

    def generate_default_recommendations(self) -> list:
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
                        model_params) -> None:
        params = {
            'warehouse_partition': warehouse_partition,
            'recommendation_count': recommendation_count,
            'model_params': model_params
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
