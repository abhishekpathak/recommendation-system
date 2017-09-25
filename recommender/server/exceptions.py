# -*- coding: utf-8 -*-

import logging

from flask import jsonify

from server import app

logger = logging.getLogger(__name__)

""" Custom HTTP based exceptions for the flask resources to use.
    Inspired from http://flask.pocoo.org/docs/0.12/patterns/apierrors/
"""


class HTTPBadRequest(Exception):
    def __init__(self, message, payload=None):
        Exception.__init__(self)

        self.message = message

        self.status_code = 400

        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())

        rv['message'] = self.message

        return rv


class HTTPInternalServerError(Exception):
    def __init__(self, message, payload=None):
        Exception.__init__(self)

        self.message = message

        self.status_code = 500

        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())

        rv['message'] = self.message

        return rv


@app.errorhandler(HTTPBadRequest)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())

    response.status_code = error.status_code

    return response


@app.errorhandler(HTTPInternalServerError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())

    response.status_code = error.status_code

    return response
