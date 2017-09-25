# -*- coding: utf-8 -*-
import json

from server import utils


class TestUtils(object):
    def test_deserialize_json_list(self):
        element_1 = {'a': 1, 'b': 2}
        element_2 = {'this test': 'good one'}
        element_3 = ["the quick brown fox"]

        data = [
            json.dumps(element_1),
            json.dumps(element_2),
            json.dumps(element_3)
        ]

        expected = [
            element_1,
            element_2,
            element_3
        ]

        actual = utils.deserialize_json_list(data)

        assert actual == expected
