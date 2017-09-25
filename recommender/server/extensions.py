# -*- coding: utf-8 -*-
import redis

from server import app, config
from server.settings.celery_conf import make_celery

""" Various global objects that can be loaded on demand"""

redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT)

celery = make_celery(app)
