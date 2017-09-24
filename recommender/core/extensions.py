# -*- coding: utf-8 -*-

import redis

from core import config

redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT)