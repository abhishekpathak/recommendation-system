# -*- coding: utf-8 -*-

import logging

from core.exceptions import ParserError

from core.datasources.base_source import BaseSource

logger = logging.getLogger(__name__)


class MovieLensSource(BaseSource):
    def __init__(self, name, ratings_file=None, products_file=None,
                 encoding='ISO-8859-1'):
        super().__init__(name, ratings_file, products_file, encoding)

    def ratings_parser(self, line: str) -> dict:
        try:
            assert line
        except AssertionError:
            raise ParserError("invalid line.")

        fields = line.strip().split("::")

        try:
            assert len(fields) == 4
        except AssertionError:
            raise ParserError("unable to find fields in line.")

        data_type = self._get_type(int(fields[3]))

        return {
            'metadata': {
                'type': data_type
            },
            'payload': {
                self.user_col: int(fields[0]),
                self.product_col: int(fields[1]),
                self.ratings_col: float(fields[2])
            }
        }

    def product_parser(self, line: str) -> dict:
        try:
            assert line
        except AssertionError:
            raise ParserError("invalid line.")

        fields = line.strip().split('::')

        try:
            assert len(fields) == 3
        except AssertionError:
            raise ParserError("unable to find fields in line.")

        return {
            self.product_col: int(fields[0]),
            'name': fields[1],
            'desc': fields[2]
        }

    @staticmethod
    def _validate_name(name):
        unwanted_chars = (' ', '-')

        for char in unwanted_chars:
            if char in name:
                message = "unwanted char {} in source name {}".format(char,
                                                                      name)
                logger.error(message)
                raise AssertionError(message)

    @staticmethod
    def _get_type(timestamp: int) -> str:
        timestamp_hash = timestamp % 10

        if timestamp_hash < 6:
            data_type = "training"
        elif 6 <= timestamp_hash < 8:
            data_type = "validation"
        else:
            data_type = "test"

        return data_type
