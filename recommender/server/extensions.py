# -*- coding: utf-8 -*-

import logging

import redis

from core.extensions import warehouse
from server import app, config
from server.settings.celery_conf import make_celery

logger = logging.getLogger(__name__)

redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT)

celery = make_celery(app)

engine_path = '{}/{}/core_app_{}'.format(config.PROJECT_ROOT,
                                         'warehouse_dir/models',
                                         warehouse.partition)
