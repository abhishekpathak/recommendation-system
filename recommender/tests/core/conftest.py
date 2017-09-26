# -*- coding: utf-8 -*-
import pytest

from core.data_loader import DataLoader
from core.datasources.movielens_source import MovieLensSource
from core.warehouse import FileWarehouse


@pytest.fixture(scope='module')
def source():
    return MovieLensSource(name='movielens_test',
                     ratings_file='external_data/test/ratings.dat',
                     products_file='external_data/test/products.dat')


@pytest.fixture(scope='module')
def warehouse(source):
    warehouse = FileWarehouse(partition=source.name)

    return warehouse


