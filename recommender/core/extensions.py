# -*- coding: utf-8 -*-

import redis

from core import config
from core.datasources.movielens_source import MovieLensSource
from core.warehouse import FileWarehouse

""" Various global objects that can be loaded on demand"""

redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT)

_source = MovieLensSource(name='movielens',
                          ratings_file='external_data/ml-1m/ratings.dat',
                          products_file='external_data/ml-1m/movies.dat',
                          encoding='ISO-8859-1')

# needed for the server component
warehouse = FileWarehouse(partition=_source.name)
