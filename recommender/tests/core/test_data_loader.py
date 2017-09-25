# -*- coding: utf-8 -*-

import pytest

from core.data_loader import DataLoader
from core.datasources.movielens_source import MovieLensSource
from core.warehouse import FileWarehouse
from core import models


class TestDataLoader(object):

    @pytest.fixture
    def source(self):
        return MovieLensSource(name='movielens_test',
                         ratings_file='external_data/ml-1m/ratings.dat',
                         products_file='external_data/ml-1m/movies.dat')

    @pytest.fixture
    def warehouse(self, mocker, source):
        warehouse = FileWarehouse(partition=source.name)
        mocker.patch.object(warehouse, 'write_row')
        return warehouse

    @pytest.fixture
    def data_loader(self, warehouse, source):
        data_loader = DataLoader(source=source, warehouse=warehouse)
        return data_loader

    def test_ratings_loader_writes_to_warehouse(self, data_loader):
        data_loader.warehouse.cleanup()
        data_loader.create_ratings_data_in_warehouse()
        data_loader.warehouse.write_row.assert_called()

    def test_product_loader_writes_to_warehouse(self, data_loader):
        data_loader.warehouse.cleanup()
        data_loader.create_product_catalog_in_warehouse()
        data_loader.warehouse.write_row.assert_called()

    def test_product_loader_writes_to_db(self, mocker, data_loader):
        mock_product = mocker.patch('core.models.Products')
        data_loader.create_product_catalog_in_serving_layer()
        mock_product.upsert.assert_called()