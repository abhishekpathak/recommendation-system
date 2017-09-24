# -*- coding: utf-8 -*-

import logging

from core.data_loader import DataLoader
from core.warehouse import FileWarehouse

logger = logging.getLogger(__name__)


from core.datasources.movielens_source import MovieLensSource

source = MovieLensSource('movielens', ratings_file='external_data/ml-1m/ratings.dat', products_file='external_data/ml-1m/movies.dat')

warehouse = FileWarehouse(partition=source.name)

dataloader = DataLoader(warehouse=warehouse, source=source)

dataloader.create_product_catalog_in_serving_layer()
