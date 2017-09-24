# -*- coding: utf-8 -*-
from logging.config import dictConfig

from core.settings import dev as config

import yaml


def load_logging_config():
    log_config_file = config.log_config_file
    with open(log_config_file) as fl:
        dictConfig(yaml.load(fl))

load_logging_config()