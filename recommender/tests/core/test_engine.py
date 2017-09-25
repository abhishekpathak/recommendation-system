# -*- coding: utf-8 -*-

import pytest

from core.datasources.movielens_source import MovieLensSource
from core.engines import ALSRecommendationEngine
from core.warehouse import FileWarehouse
from mock import MagicMock


class TestRecommendationEngine(object):

    @pytest.fixture
    def warehouse(self, mocker, source):
        warehouse = MagicMock()

        return warehouse

    @pytest.fixture
    def engine(self, warehouse):
        engine = ALSRecommendationEngine(warehouse=warehouse)

        return engine

    def test_ready_no_params(self, engine):
        engine.als_params = {}

        assert not engine.ready()

    def test_ready_no_model(self, engine):
        engine.model = None

        assert not engine.ready()

