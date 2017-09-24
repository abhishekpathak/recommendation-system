# -*- coding: utf-8 -*-
import json
from collections import Generator

from core import config
from core import utils

from core.extensions import redis_conn


class Users(object):
    def __init__(self, id: int, data_source: str):
        self.id = id
        self.data_source = data_source

    @classmethod
    def get(cls, id: int, data_source: str):
        if id in config.ALLOWED_USER_IDS:
            return cls(id, data_source)
        else:
            raise KeyError("user with id: {} does not exist.".format(id))

    @classmethod
    def get_all(cls, data_source: str) -> Generator:
        return (cls.get(user_id, data_source) for user_id in
                config.ALLOWED_USER_IDS)

    def get_ratings(self) -> list:
        key = '{}_ratings_{}'.format(self.data_source, self.id)

        return utils.deserialize_json_list(redis_conn.lrange(key, 0, -1))

    def get_products_used(self) -> list:
        key = '{}_products_{}'.format(self.data_source, self.id)

        return redis_conn.lrange(key, 0, -1)

    def get_recommendations(self) -> list:
        key = '{}_recommendations_{}'.format(self.data_source, self.id)

        return json.loads(redis_conn.get(key))

    def set_recommendations(self, recommendations: list) -> None:
        key = '{}_recommendations_{}'.format(self.data_source, self.id)

        value = json.dumps(recommendations)

        redis_conn.set(key, value)

    def set_ratings(self, ratings: list) -> None:
        key = 'ratings_{}'.format(self.id)

        # serialise individual rating inside ratings
        value = [json.dumps(rating) for rating in ratings]

        redis_conn.rpush(key, *value)

    def has_rated(self):
        key = '{}_ratings_{}'.format(self.data_source, self.id)

        return redis_conn.get(key) is not None


class Products(object):
    def __init__(self, data_source: str, id: int, name: str, desc: str):
        self.data_source = data_source
        self.id = id
        self.name = name
        self.desc = desc

    @classmethod
    def get(cls, id: int, data_source: str):
        key = '{}_products_{}'.format(data_source, id)

        meta = redis_conn.get(key)

        return cls(id=id, name=meta['name'], desc=meta['desc'])

    @classmethod
    def get_all(cls, data_source) -> Generator:
        # products catalog can be large, use a generator
        return (cls.get(key, data_source) for key in
                redis_conn.keys('{}_products_*'.format(data_source)))

    @classmethod
    def upsert(cls, data_source, id, name, desc) -> None:
        key = '{}_products_{}'.format(data_source, id)

        value = json.dumps({
            'name': name,
            'desc': desc
        })

        redis_conn.set(key, value)
