# -*- coding: utf-8 -*-
import os

PROJECT_ROOT = os.getcwd()

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

CELERY_BROKER_URL = 'redis://localhost:6379/0',
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

# models
ALLOWED_USER_IDS = [-1, 10001, 10002]

log_config_file = '{}/{}/settings/log.yaml'.format(PROJECT_ROOT, 'server')