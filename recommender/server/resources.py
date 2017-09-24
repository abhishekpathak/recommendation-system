# -*- coding: utf-8 -*-
import logging
from http import HTTPStatus

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from core.engine import ALSRecommendationEngine
from server import config
from server import tasks, api

logger = logging.getLogger(__name__)


class EngineResource(Resource):

    def get(self, engine_id: str):
        current_engine = ALSRecommendationEngine.import_from_path(config.engine_path)

        response_body = {
            'warehouse_partition': current_engine.warehouse.partition,
            'recommendation count': current_engine.recommendation_count,
            'ALS parameters': current_engine.als_params,
        }
        response_status = HTTPStatus.OK
        response_headers = None
        return response_body, response_status, response_headers


class EnginesResource(Resource):

    def post(self):
        try:
            als_opts = request.get_json()['als_opts']
        except (BadRequest, KeyError):
            message = 'missing payload: als_opts'
            logger.error(message)
            response_body = {
                'message': message
            }
            response_status = HTTPStatus.BAD_REQUEST
            response_headers = None
            return response_body, response_status, response_headers

        try:
            self._validate_als_opts(als_opts)
        except AssertionError as e:
            message = 'invalid als_opts: {}'.format(e)
            logger.error(message)
            response_body = {
                'message': message
            }
            response_status = HTTPStatus.BAD_REQUEST
            response_headers = None
            return response_body, response_status, response_headers

        task = tasks.train_new_model.delay(config.engine_path, **als_opts)
        message = 'new job created with id {}'.format(task.id)
        logger.info(message)
        response_body = {
            'message': message
        }
        response_status = HTTPStatus.ACCEPTED
        response_headers = {
            'Location': api.url_for(TaskResource, task_id=task.id)
        }
        return response_body, response_status, response_headers

    @staticmethod
    def _validate_als_opts(als_opts: dict) -> None:
        required_keys = {'rank_opts', 'reg_param_opts', 'max_iter_opts'}

        assert set(als_opts.keys()) == required_keys, 'missing required keys.'

        for key in als_opts.keys():
            assert isinstance(als_opts[key], list), 'key {} is not a tuple.'.format(key)


class TaskResource(Resource):

    def get(self, task_id: str):
        task = tasks.train_new_model.AsyncResult(task_id)
        if task.state == 'SUCCESS':
            response_data = {
                'state': task.state
            }
            response_status = HTTPStatus.SEE_OTHER
            response_headers = {
                'Location': api.url_for(EngineResource, engine_id='current')
            }
            return response_data, response_status, response_headers
        else:
            response = {
                    'state': task.state,
                }
        return response