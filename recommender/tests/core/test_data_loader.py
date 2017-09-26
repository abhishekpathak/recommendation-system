# -*- coding: utf-8 -*-

import pytest

from core.data_loader import DataLoader


class TestDataLoader(object):

    @pytest.fixture
    def warehouse(self, mocker, warehouse):
        mocker.patch.object(warehouse, 'write_row')
        return warehouse

    @pytest.fixture
    def data_loader(self, warehouse, source):
        loader = DataLoader(source=source, warehouse=warehouse)

        return loader

    def test_ratings_loader_writes_to_warehouse(self, data_loader):
        data_loader.warehouse.cleanup()

        data_loader.create_ratings_data_in_warehouse()

        data_loader.warehouse.write_row.assert_called()

    def test_product_loader_writes_to_warehouse(self, data_loader):
        data_loader.warehouse.cleanup()

        data_loader.create_product_catalog_in_warehouse()

        data_loader.warehouse.write_row.assert_called()

    def test_product_loader_writes_to_db(self, mocker, data_loader):
        mock_product = mocker.patch.object(data_loader, 'products_model')

        data_loader.create_product_catalog_in_serving_layer()

        mock_product.upsert.assert_called()
