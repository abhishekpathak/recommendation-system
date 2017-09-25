# -*- coding: utf-8 -*-
import pytest
from mock import MagicMock

from core.transporter import Transporter


class TestTransporter(object):

    @pytest.fixture
    def transporter(self):
        warehouse = MagicMock()

        transporter = Transporter(warehouse=warehouse)

    def test_send_new_ratings_to_warehouse(self):
        pass

    def test_send_recommendations_to_db(self):
        pass

    def test_send_users_to_warehouse(self):
        pass