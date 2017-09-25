# -*- coding: utf-8 -*-
import json
import logging

from core.models import Users
from core.warehouse import FileWarehouse
from core import config

logger = logging.getLogger(__name__)


class Transporter(object):
    """ This class represents the data pipeline.

    A Transporter is responsible for keeping data in sync between the serving
    database and the warehouse.

    Typical roles include:
    * pick fresh ratings from the serving db and dump to warehouse
    * pick fresh recommendations from the warehouse and dump to serving db
    * keep the global list of active users in sync
    * keep the global list of products in sync

    The methods on this class can be called upon periodically to perform the
    above roles. Ideally, the methods should be be idempotent.

    Attributes:
        warehouse: a warehouse instance
        user_model: a user model (to populate the serving db)
        (not implemented) product_model: a product model (to populate the
        serving db)
    """
    def __init__(self, warehouse: FileWarehouse, user_model: Users):
        self.warehouse = warehouse
        self.user_model = user_model

    def send_new_ratings_to_warehouse(self) -> None:
        """ picks newly added ratings from the serving db and adds to the
        warehouse.

        """
        users = self.user_model.get_all(data_partition=self.warehouse.partition)

        for user in users:

            if user.has_rated():
                ratings = user.get_ratings()
                logger.debug('User {} has ratings {}'.format(user.id, ratings))
            else:
                logger.debug('User {} has not rated anything.'.format(user.id))
                continue

            transformed_ratings = [self._transform_rating(user, item) for item
                                   in ratings]

            logger.info('sending ratings: {} to warehouse'
                         .format(transformed_ratings))

            self.warehouse.update_ratings(transformed_ratings)

    def send_recommendations_to_db(self) -> None:
        """ picks recommendations from the warehouse and adds it to the
        serving db.

        """
        with open(self.warehouse.recommendations_file) as recommendations:

            for recommendation in recommendations:

                user_id, transformed_recommendation = \
                    self._transform_recommendation(recommendation)
                user = self.user_model.get(id=user_id,
                                 data_partition=self.warehouse.partition)
                user.set_recommendations(transformed_recommendation)
                logger.debug('recommendations set for user {}: {}'
                             .format(user.id, transformed_recommendation))

    def send_users_to_warehouse(self) -> None:
        """ creates a global list of users (for whom recommendations need to be
        generated) from the serving db and adds it to the warehouse.

        """
        users = self.user_model.get_all(data_partition=self.warehouse.partition)

        transformed_users = []

        for user in users:
            # TODO actually this should be user.has_used_anything()
            if user.has_rated():
                transformed_users.append(self._transform_user(user))
                logger.debug('sending user {} to warehouse.'.format(user.id))
            else:
                logger.debug('user {} has not used any product.'
                             'Not sending to warehouse.'.format(user.id))

        logger.info(
            'total users sent to warehouse: {}'.format(len(transformed_users)))

        self.warehouse.update_users(transformed_users)

    def send_products_to_warehouse(self) -> None:
        """ creates a global list of products (which are candidates for
        recommendation) and adds it to the warehouse.

        This method is not implemented. For our system, we are content with the
        initial product catalog loaded to the warehouse by the loader.
        """
        raise NotImplementedError("no support for syncing products")

    @staticmethod
    def _transform_rating(user: Users, rating: dict) -> dict:
        return {
            config.USER_COL: user.id,
            config.PRODUCT_COL: rating['product_id'],
            config.RATINGS_COL: rating['rating']
        }

    @staticmethod
    def _transform_recommendation(recommendation_str: str) -> tuple:
        recommendation = json.loads(recommendation_str.strip())

        user_id = recommendation[config.USER_COL]

        recommended_product_ids = recommendation['recommendations']

        return user_id, recommended_product_ids

    @staticmethod
    def _transform_user(user: Users) -> dict:
        return {config.USER_COL: user.id}
