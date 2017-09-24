# -*- coding: utf-8 -*-

import logging

import redis

from server import config

logger = logging.getLogger(__name__)

redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT)
