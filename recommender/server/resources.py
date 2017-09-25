# -*- coding: utf-8 -*-
import logging
from http import HTTPStatus

from flask import request, Request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from core.engines import ALSRecommendationEngine
from core.extensions import warehouse
from server import config
from server import tasks, api
from server.exceptions import HTTPBadRequest, HTTPInternalServerError

logger = logging.getLogger(__name__)

""" Set up a path for the engine to be serialized and deserialized from. """
ENGINE_PATH = '{}/{}/core_app_{}'.format(config.PROJECT_ROOT,
                                         'warehouse_dir/models',
                                         warehouse.partition)


class EngineResource(Resource):
    """ Exposes the Engine as a resource for REST.

    To support individual resource handling (via GET) as well as a resource
    group handling (via POST), we have to set up 2 different classes, dealing
    with individual and groups respectively. This is as per the design choices
    of flask_restful.

    ref : https://stackoverflow.com/q/20715238/647952

    This class handles the individual resource.

    """
    def get(self, engine_id: str):
        """ fetch the details of an engine.

        As per design, the only supported engine resource is the current one.

        Args:
            engine_id: identifier for the engine resource. Currently supports
            only 1 value - "current".
        """

        # validate the engine id
        try:
            assert engine_id.lower() == 'current'
        except AssertionError:
            message = 'Only engine resource supported is the current one.'
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        # Load the current engine.
        try:
            current_engine = ALSRecommendationEngine.import_from_path(
                ENGINE_PATH)
        except Exception as e:
            message = "Error loading the recommendation engine."
            logger.error(message, *e.args)
            raise HTTPInternalServerError(message, payload={'message': message})

        # return its parameters
        return {
            'warehouse_partition': current_engine.warehouse.partition,
            'recommendation count': current_engine.recommendation_count,
            'ALS parameters': current_engine.model_params,
        }


class EnginesResource(Resource):
    """ Exposes the Engine as a resource for REST.

    To support individual resource handling (via GET) as well as a resource
    group handling (via POST), we have to set up 2 different classes, dealing
    with individual and groups respectively. This is as per the design choices
    of flask_restful.

    ref : https://stackoverflow.com/q/20715238/647952

    This class handles the group (list?) resource.

    """
    def post(self):
        """ Train a new engine resource. When fully trained, this engine will
        be mapped to the "current" engine resource.

        """
        # validate the als options provided in request body
        try:
            assert self._has_valid_als_opts(request)
        except AssertionError:
            message = 'ALS opts seem invalid.Please check the payload.'
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        als_opts = request.get_json()['als_opts']

        # start training a new model asynchronously
        task = tasks.train_new_model.delay(ENGINE_PATH, **als_opts)

        message = 'new job created with id {}'.format(task.id)

        logger.info(message)

        response_body = {
            'message': message
        }

        response_status = HTTPStatus.ACCEPTED

        # send across a location header for the client to track the async job
        response_headers = {
            'Location': api.url_for(TaskResource, task_id=task.id)
        }
        return response_body, response_status, response_headers

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


class TaskResource(Resource):
    """ Exposes a celery task as a resource for REST.

    """
    def get(self, task_id: str):
        """ get the progress of a celery task.

        Args:
            task_id: celery task id returned by a previous API.
        """
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
