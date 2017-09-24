# -*- coding: utf-8 -*-
import logging
from http import HTTPStatus

from flask import request, Request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from core.engines import ALSRecommendationEngine
from server import tasks, api
from server.exceptions import HTTPBadRequest, HTTPInternalServerError
from server.extensions import engine_path

logger = logging.getLogger(__name__)


class EngineResource(Resource):
    def get(self, engine_id: str):
        try:
            assert engine_id.lower() == 'current'
        except AssertionError:
            message = 'Only engine resource supported is the current one.'
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        try:
            current_engine = ALSRecommendationEngine.import_from_path(
                engine_path)
        except Exception as e:
            message = "Error loading the recommendation engine."
            logger.error(message, *e.args)
            raise HTTPInternalServerError(message, payload={'message': message})

        return {
            'warehouse_partition': current_engine.warehouse.partition,
            'recommendation count': current_engine.recommendation_count,
            'ALS parameters': current_engine.als_params,
        }


class EnginesResource(Resource):
    @staticmethod
    def _has_valid_als_opts(request_obj: Request) -> bool:
        try:
            als_opts = request_obj.get_json()['als_opts']
        except (BadRequest, KeyError):
            return False

        required_keys = ('rank_opts', 'reg_param_opts', 'max_iter_opts')

        for key in required_keys:
            assert key in als_opts.keys()
            assert isinstance(als_opts[key], list)

    def post(self):
        try:
            assert self._has_valid_als_opts(request)
        except AssertionError:
            message = 'ALS opts seem invalid.Please check the payload.'
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        als_opts = request.get_json()['als_opts']

        task = tasks.train_new_model.delay(engine_path, **als_opts)

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


class TaskResource(Resource):
    def get(self, task_id: str):
        task = tasks.train_new_model.AsyncResult(task_id)

        response_body = {'state': task.state}

        if task.state == 'SUCCESS':
            response_status = HTTPStatus.SEE_OTHER
            response_headers = {
                'Location': api.url_for(EngineResource, engine_id='current')
            }
            return response_body, response_status, response_headers
        else:
            return response_body
