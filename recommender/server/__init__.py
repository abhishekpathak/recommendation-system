# -*- coding: utf-8 -*-

from flask import Flask
from flask_cors import CORS
from flask_restful import Api

import server.settings.dev as config
from core.engines import ALSRecommendationEngine
from server.settings import log

app = Flask(__name__)

CORS(app)

api = Api(app)

log.configure_logging()

from server import views

ALSRecommendationEngine._load_spark_session()
