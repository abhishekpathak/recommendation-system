# -*- coding: utf-8 -*-

from server.settings import dev as config

from logging.config import dictConfig

import yaml
from flask import Flask
from flask_restful import Api


app = Flask(__name__)

api = Api(app)


def load_logging_config():
    log_config_file = config.log_config_file
    with open(log_config_file) as fl:
        dictConfig(yaml.load(fl))

load_logging_config()

from server import views