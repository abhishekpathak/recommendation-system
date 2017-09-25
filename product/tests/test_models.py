# -*- coding: utf-8 -*-
import pytest
from mock import MagicMock

from server.models import DATA_PARTITION
from server.models import Products


class TestProducts(object):
    """ Test the products resource. """

    @pytest.fixture
    def products_model(self):
        model = Products

        model.redis = MagicMock()

        model.redis.get.return_value = b'{"name": "Chungking Express (1994)",' \
                                       b' "desc": "Drama|Mystery|Romance"}'

        return model

    def test_get(self, products_model):
        p_id = '123'

        products_model.get(id=p_id)

        products_model.redis.get.assert_called_once_with(
            '{}_products_123'.format(DATA_PARTITION))

    def test_upsert(self, products_model):
        products_model.upsert(id=123, name='testname', desc='testdesc')

        products_model.redis.set.assert_called_once()
