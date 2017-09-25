# -*- coding: utf-8 -*-
import json
from collections import Generator

from server import config
from server.extensions import redis_conn


""" A simple, quickly-prototyped ORM layer on top of Redis. 
Supports all the functions needed by the serving layer, and nothing more.
The implementation details are hackish, and best left alone.
"""


class Users(object):
    """ A model that represents the user.

    Supports the following queries:
    * get a user from id
    * get all users in the system
    * get the ratings given by a user
    * persist the given ratings for a user
    * get all the products used by a user
    * get the recommendations for a user

    """
    redis = redis_conn

    def __init__(self, id: int, data_partition: str):
        self.id = id
        self.data_partition = data_partition

    @classmethod
    def get(cls, id: int, data_partition: str):
        if id in config.ALLOWED_USER_IDS:
            return cls(id=id, data_partition=data_partition)
        else:
            raise KeyError("user with id: {} does not exist.".format(id))

    @classmethod
    def get_all(cls, data_partition: str) -> list:
        return [cls.get(id=user_id, data_partition=data_partition) for user_id
                in config.ALLOWED_USER_IDS]

    def get_ratings(self) -> list:
        key = '{}_ratings_{}'.format(self.data_partition, self.id)

        ratings_hash = self.redis.hgetall(key)

        ratings = []

        for key, value in ratings_hash.items():
            ratings.append({
                'product_id': int(key),
                'rating': int(value)
            })

        return ratings

    def get_products_used(self) -> list:
        ratings = self.get_ratings()

        return [item['product_id'] for item in ratings]

    def get_recommendations(self) -> list:
        key = '{}_recommendations_{}'.format(self.data_partition, self.id)

        return self._get_recommendations_for_key(key)

    def set_recommendations(self, recommendations: list) -> None:
        key = '{}_recommendations_{}'.format(self.data_partition, self.id)

        value = json.dumps(recommendations)

        self.redis.set(key, value)

    @classmethod
    def _get_recommendations_for_key(cls, key):
        value = cls.redis.get(key)

        recommendations = json.loads(value) if value else []

        return recommendations

    @classmethod
    def get_default_recommendations(cls, data_partition: str):
        key = '{}_recommendations_-1'.format(data_partition)

        return cls._get_recommendations_for_key(key)

    def set_ratings(self, ratings: list) -> None:
        key = '{}_ratings_{}'.format(self.data_partition, self.id)

        for rating in ratings:
            self.redis.hset(key, rating['product_id'], rating['rating'])

    def has_rated(self):
        return self.get_ratings() != []


class Products(object):
    """ A model that represents the Product.

    Supports the following queries:
    * get a product from id
    * get all products in the system
    * add a new product to the system. If the product already exists, update it.

    """

    redis = redis_conn

    def __init__(self, id: int, name: str, desc: str):
        self.id = id
        self.name = name
        self.desc = desc

    @classmethod
    def get(cls, id: int, data_partition: str):
        key = '{}_products_{}'.format(data_partition, id)

        meta = cls.redis.get(key)

        if meta:
            meta = json.loads(meta)

            return cls(id=id, name=meta['name'], desc=meta['desc'])

    @classmethod
    def get_all(cls, data_partition: str) -> Generator:
        # products catalog can be large, use a generator
        return (cls.get(id=key, data_partition=data_partition) for key in
                cls.redis.keys('{}_products_*'.format(data_partition)))

    @classmethod
    def upsert(cls, id, name, desc, data_partition: str) -> None:
        key = '{}_products_{}'.format(data_partition, id)

        value = json.dumps({
            'name': name,
            'desc': desc
        })

        cls.redis.set(key, value)
