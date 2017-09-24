# -*- coding: utf-8 -*-

import redis

from core import config
from core.datasources.movielens_source import MovieLensSource
from core.warehouse import FileWarehouse

redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT)

source = MovieLensSource(name='movielens',
                         ratings_file='external_data/ml-1m/ratings.dat',
                         products_file='external_data/ml-1m/movies.dat',
                         encoding='ISO-8859-1')

warehouse = FileWarehouse(partition=source.name)
