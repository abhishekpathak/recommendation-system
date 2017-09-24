# -*- coding: utf-8 -*-

import logging

from core.data_loader import DataLoader
from core.datasources.movielens_source import MovieLensSource
from core.engines import ALSRecommendationEngine
from core.transporter import Transporter
from core.warehouse import FileWarehouse
from core import config

logger = logging.getLogger(__name__)

source = MovieLensSource('movielens',
                         ratings_file='external_data/ml-1m/ratings.dat',
                         products_file='external_data/ml-1m/movies.dat')

warehouse = FileWarehouse(partition=source.name)

transporter = Transporter(warehouse=warehouse)

warehouse.cleanup()

dataloader = DataLoader(warehouse=warehouse, source=source)

dataloader.create_product_catalog_in_serving_layer()

dataloader.create_product_catalog_in_warehouse()

dataloader.create_ratings_data_in_warehouse()

engine = ALSRecommendationEngine(warehouse=warehouse)

engine.train_new_model(**config.als_opts)

#engine = ALSRecommendationEngine.import_from_path(config.MODELS_DIR + "/core_app_movielens")

transporter.send_users_to_warehouse()

transporter.send_new_ratings_to_warehouse()

engine.retrain_with_updated_data()

engine.generate_recommendations()

transporter.send_recommendations_to_db()
