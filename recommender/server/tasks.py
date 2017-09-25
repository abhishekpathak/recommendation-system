# -*- coding: utf-8 -*-

import logging

from core.engines import ALSRecommendationEngine
from server.extensions import celery

logger = logging.getLogger(__name__)


@celery.task(bind=True)
def train_new_model(self, engine_path: str, **als_opts: dict):
    """ Trains a new engine instance. Stateless in nature.

    Args:
        engine_path: path from which engine can be loaded
        als_opts : parameter options for the ALS model.

    """
    engine = ALSRecommendationEngine.import_from_path(engine_path)

    data = engine.train_new_model(**als_opts)

    engine.export(path=engine_path)

    return data


@celery.task(bind=True)
def retrain_engine(self, engine_path: str):
    """ Retrains an existing engine. Stateless in nature.

    Args:
        engine_path: path from which engine can be loaded

    """
    engine = ALSRecommendationEngine.import_from_path(engine_path)

    engine.retrain_with_updated_data()

    engine.export(path=engine_path)


@celery.task(bind=True)
def generate_recommendations(self, engine_path: str):
    """ Generates recommendations. Stateless in nature.

    Args:
        engine_path: path from which engine can be loaded

    """
    engine = ALSRecommendationEngine.import_from_path(engine_path)

    engine.generate_recommendations()

    engine.export(path=engine_path)
