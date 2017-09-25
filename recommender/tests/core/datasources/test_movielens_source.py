# -*- coding: utf-8 -*-

import pytest

from core.datasources.movielens_source import MovieLensSource
from core.exceptions import ParserError


class TestMovieLensSource(object):

    @pytest.fixture
    def source(self):
        source = MovieLensSource('test')
        return source

    def test_ratings_parser_throws_error_null_input(self, source):
        line = None

        with pytest.raises(ParserError):
            source.ratings_parser(line)

    def test_ratings_parser_throws_error_invalid_input(self, source):
        line = "this is a sample line:: :: what's up!"

        with pytest.raises(ParserError):
            source.ratings_parser(line)

    def test_ratings_parser_parses_fields(self, source):
        line = "1::2804::5::978300719"

        expected = {
            'metadata': {
                'type': 'test'
            },
            'payload': {
                'user_id': 1,
                'product_id': 2804,
                'ratings': 5
            }
        }

        actual = source.ratings_parser(line)

        assert expected == actual

    def test_product_parser_throws_error_null_input(self, source):
        line = None

        with pytest.raises(ParserError):
            source.product_parser(line)

    def test_product_parser_throws_error_invalid_input(self, source):
        line = "this is a sample line:: :: what's up!"

        with pytest.raises(ParserError):
            source.product_parser(line)

    def test_product_parser_parses_fields(self, source):
        line = "1::Toy Story (1995)::Animation|Children's|Comedy"

        expected = {
            "product_id": 1,
            'name': "Toy Story (1995)",
            'desc': "Animation|Children's|Comedy"
        }

        actual = source.product_parser(line)

        assert expected == actual