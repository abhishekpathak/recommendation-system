# -*- coding: utf-8 -*-
import json
import logging

from core.models import Users
from core.warehouse import FileWarehouse
from core import config

logger = logging.getLogger(__name__)


class Transporter(object):
    def __init__(self, warehouse: FileWarehouse):
        self.warehouse = warehouse

    def send_new_ratings_to_warehouse(self) -> None:
        users = Users.get_all(data_partition=self.warehouse.partition)

        new_ratings = []

        for user in users:

            if user.has_rated():
                ratings = user.get_ratings()
                logger.debug('User {} has ratings {}'.format(user.id, ratings))
            else:
                logger.debug('User {} has not rated anything.'.format(user.id))
                continue

            for rating in ratings:
                transformed_rating = self._transform_rating(user, rating)
                new_ratings.append(transformed_rating)
                logger.debug('sending rating: {} to warehouse'.format(
                    transformed_rating))

        self.warehouse.update_ratings(new_ratings)

    def send_recommendations_to_db(self) -> None:
        with open(self.warehouse.recommendations_file) as recommendations:
            for recommendation in recommendations:
                user_id, transformed_recommendation = \
                    self._transform_recommendation(recommendation)
                user = Users.get(id=user_id,
                                 data_partition=self.warehouse.partition)
                user.set_recommendations(transformed_recommendation)
                logger.debug('recommendations set for user {}: {}'
                             .format(user.id, transformed_recommendation))

    def send_users_to_warehouse(self) -> None:
        # send all users who have used products (even if they have not rated)
        # to the warehouse, to generate recommendations
        users = Users.get_all(data_partition=self.warehouse.partition)

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
