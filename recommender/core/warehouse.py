# -*- coding: utf-8 -*-
import json
import logging
from abc import ABC, abstractmethod
from io import TextIOWrapper

from core import config, utils
from core.exceptions import WarehouseException

logger = logging.getLogger(__name__)


class Warehouse(ABC):
    @abstractmethod
    def update_ratings(self, new_ratings):
        pass

    @abstractmethod
    def update_users(self, users):
        pass


class FileWarehouse(Warehouse):
    def __init__(self, partition: str):
        self.partition = partition
        self.root_path = '{}/{}'.format(config.WAREHOUSE_DATA_DIR,
                                        self.partition)
        self.ratings_file = '{}/ratings'.format(self.root_path)
        self.training_file = '{}/training'.format(self.root_path)
        self.test_file = '{}/test'.format(self.root_path)
        self.validation_file = '{}/validation'.format(self.root_path)
        self.products_file = '{}/products'.format(self.root_path, )
        self.recommendations_file = '{}/recommendations'.format(self.root_path)
        self.users_file = '{}/users'.format(self.root_path)

    def cleanup(self):
        # use this during first-time setup of warehouse
        utils.delete_directory(self.root_path)
        utils.create_directory(self.root_path)
        for file in (
                self.ratings_file,
                self.training_file,
                self.test_file,
                self.validation_file,
                self.products_file,
                self.recommendations_file,
                self.users_file
        ):
            utils.touch_file(file)

    def update_ratings(self, new_ratings: list) -> None:
        # TODO deduplication
        try:
            with open(self.ratings_file, 'a') as ratings_file:
                for rating in new_ratings:
                    self.write_row(ratings_file, rating)
        except IOError as e:
            message = "Unable to update ratings. Error reported:{}".format(e)
            logger.error(message)
            raise WarehouseException(message)

    def update_users(self, users: list) -> None:
        try:
            with open(self.users_file, 'w') as users_file:
                for user in users:
                    self.write_row(users_file, user)
        except IOError as e:
            message = "Unable to update users. Error reported:{}".format(e)
            logger.error(message)
            raise WarehouseException(message)

    def update_recommendations(self, user_id: int,
                               recommendations: list) -> None:
        # TODO ideally update the older recommendation of this user
        recommendation_dict = {
            config.USER_COL: user_id,
            'recommendations': recommendations
        }

        try:
            with open(self.recommendations_file, 'a') as recommendations_file:
                self.write_row(recommendations_file, recommendation_dict)
        except IOError as e:
            message = "Unable to update users. Error reported:{}".format(e)
            logger.error(message)
            raise WarehouseException(message)

    @staticmethod
    def write_row(file_handle: TextIOWrapper, data: dict) -> None:
        json.dump(data, file_handle)
        file_handle.write('\n')
