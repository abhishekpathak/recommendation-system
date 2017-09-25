# -*- coding: utf-8 -*-
from flask import Flask
from flask_cors import CORS
from flask_restful import Api

from server.settings import dev as config
from server.settings import log

app = Flask(__name__)

# enable CORS for this app
CORS(app)

api = Api(app)

log.configure_logging()

from server import views
