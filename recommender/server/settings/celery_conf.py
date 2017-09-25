# -*- coding: utf-8 -*-
from celery import Celery

from server import config

""" Celery configuration module.

"""

def make_celery(app):
    """ Flask integration with celery. Taken from
    http://flask.pocoo.org/docs/0.12/patterns/celery/

    """
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
