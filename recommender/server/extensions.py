# -*- coding: utf-8 -*-

import logging

import redis
from celery import Celery
from core.engine import ALSRecommendationEngine
from core.warehouse import FileWarehouse

from core.datasources.movielens_source import MovieLensSource
from server import app, config

logger = logging.getLogger(__name__)

redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT)

def make_celery(app):
    celery = Celery(app.import_name, backend=config.CELERY_RESULT_BACKEND, broker=config.CELERY_BROKER_URL)
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

celery = make_celery(app)

source = MovieLensSource(name='movielens',
                         ratings_file='external_data/ml-1m/ratings.dat',
                         products_file='external_data/ml-1m/movies.dat',
                         encoding='ISO-8859-1')

warehouse = FileWarehouse(partition=source.name)

config.engine_path = '{}/{}/core_app_{}'.format(config.PROJECT_ROOT, 'warehouse_dir/models', warehouse.partition)

ALSRecommendationEngine._load_spark_session()