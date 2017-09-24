# -*- coding: utf-8 -*-
import json
from collections import Generator

from server import config, data_partition
from server.extensions import redis_conn


class Users(object):
    def __init__(self, id: int):
        self.id = id

    @classmethod
    def get(cls, id: int):
        if id in config.ALLOWED_USER_IDS:
            return cls(id)
        else:
            raise KeyError("user with id: {} does not exist.".format(id))

    @classmethod
    def get_all(cls) -> Generator:
        return (cls.get(user_id) for user_id in
                config.ALLOWED_USER_IDS)

    def get_ratings(self) -> list:
        key = '{}_ratings_{}'.format(data_partition, self.id)

        ratings_hash = redis_conn.hgetall(key)

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
        key = '{}_recommendations_{}'.format(data_partition, self.id)

        return self._get_recommendations_for_key(key)

    @staticmethod
    def _get_recommendations_for_key(key):
        value = redis_conn.get(key)

        if value:
            recommendations = json.loads(value)
        else:
            recommendations = []

        return recommendations

    @classmethod
    def get_default_recommendations(cls):
        key = '{}_recommendations_-1'.format(data_partition)

        return cls._get_recommendations_for_key(key)

    def set_ratings(self, ratings: list) -> None:
        key = '{}_ratings_{}'.format(data_partition, self.id)

        for rating in ratings:
            redis_conn.hset(key, rating['product_id'], rating['rating'])

    def has_rated(self):
        key = '{}_ratings_{}'.format(data_partition, self.id)

        return redis_conn.get(key) is not None


class Products(object):
    def __init__(self, id: int, name: str, desc: str):
        self.id = id
        self.name = name
        self.desc = desc

    @classmethod
    def get(cls, id: int):
        key = '{}_products_{}'.format(data_partition, id)

        meta = redis_conn.get(key)

        if meta:
            meta = json.loads(meta)

            return cls(id=id, name=meta['name'], desc=meta['desc'])

    @classmethod
    def get_all(cls) -> Generator:
        # products catalog can be large, use a generator
        return (cls.get(key) for key in
                redis_conn.keys('{}_products_*'.format(data_partition)))

    @classmethod
    def upsert(cls, id, name, desc) -> None:
        key = '{}_products_{}'.format(data_partition, id)

        value = json.dumps({
            'name': name,
            'desc': desc
        })

        redis_conn.set(key, value)
