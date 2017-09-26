# -*- coding: utf-8 -*-

import pytest
from mock import MagicMock

from core.engines import ALSRecommendationEngine


class TestRecommendationEngine(object):
    @pytest.fixture
    def warehouse(self, source):
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
