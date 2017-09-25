# -*- coding: utf-8 -*-
import redis

from server import config

""" Various global objects that can be loaded on demand"""

redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT)
