# -*- coding: utf-8 -*-
import os

PROJECT_ROOT = os.getcwd()

WAREHOUSE_ROOT = '{}/warehouse_dir'.format(PROJECT_ROOT)

WAREHOUSE_DATA_DIR = '{}/data'.format(WAREHOUSE_ROOT)

MODELS_DIR = '{}/models'.format(WAREHOUSE_ROOT)

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

CELERY_BROKER_URL = 'redis://localhost:6379',
CELERY_RESULT_BACKEND = 'redis://localhost:6379'

# warehouse
USER_COL = 'user_id'
PRODUCT_COL = 'product_id'
RATINGS_COL = 'ratings'
DEFAULT_USERID = -1

# models
ALLOWED_USER_IDS = [-1, 10001, 10002]

# engine
als_opts = {
    'rank_opts': [6, 8, 10, 12],
    'reg_param_opts': [0.1, 1.0, 5.0, 10.0],
    'max_iter_opts': [3, 10, 20]
}

log_config_file = '{}/{}/settings/log.yaml'.format(PROJECT_ROOT, 'core')