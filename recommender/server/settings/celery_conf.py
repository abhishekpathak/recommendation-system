# -*- coding: utf-8 -*-

import logging

from celery import Celery

logger = logging.getLogger(__name__)

from server import config


def make_celery(app):
    celery = Celery(app.import_name, backend=config.CELERY_RESULT_BACKEND,
                    broker=config.CELERY_BROKER_URL)
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    return celery
