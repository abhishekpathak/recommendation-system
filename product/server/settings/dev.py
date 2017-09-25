# -*- coding: utf-8 -*-

import os

PROJECT_ROOT = os.getcwd()

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# models


# logging
log_config_file = '{}/{}/settings/log.yaml'.format(PROJECT_ROOT, 'server')
