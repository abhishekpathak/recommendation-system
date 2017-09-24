# -*- coding: utf-8 -*-

from flask import Flask
from flask_cors import CORS
from flask_restful import Api

import server.settings.dev as config
from server.settings import log

app = Flask(__name__)

CORS(app)

api = Api(app)

log.configure_logging()

data_partition = 'movielens'

from server import views
