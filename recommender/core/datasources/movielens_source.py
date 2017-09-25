# -*- coding: utf-8 -*-

import logging

from core.datasources.base_source import BaseSource
from core.exceptions import ParserError

logger = logging.getLogger(__name__)


class MovieLensSource(BaseSource):
    """ The movielens data source, https://grouplens.org/datasets/movielens/1m/.

    Implements the `BaseSource` contract. For more details see `BaseSource`.

    Attributes:
        same as `BaseSource`.
    """

    def __init__(self, name: str, ratings_file: str, products_file: str,
                 encoding='ISO-8859-1'):
        """ No magic here, just wraps up the call to the superclass.

        Args:
             same as `BaseSource.__init__`.

        """
        super().__init__(name, ratings_file, products_file, encoding)

    def ratings_parser(self, line: str) -> dict:
        """ Implements the ratings parser as defined in the base source.

        Args:
            same as `BaseSource.ratings_parser`

        Returns:
            same as `BaseSource.ratings_parser`

        """
        try:
            assert line
        except AssertionError as e:
            raise ParserError("Invalid line.") from e

        fields = line.strip().split("::")

        try:
            # there are 4 fields in the movielens ratings line
            assert len(fields) == 4
        except AssertionError as e:
            raise ParserError("Unable to find 4 fields in the line.") from e

        return {
            'metadata': {
                'type': self._get_type(int(fields[3]))
            },
            'payload': {
                self.user_col: int(fields[0]),
                self.product_col: int(fields[1]),
                self.ratings_col: float(fields[2])
            }
        }

    def product_parser(self, line: str) -> dict:
        """ Implements the product parser as defined in the base source.

        Args:
            same as `BaseSource.product_parser`

        Returns:
            same as `BaseSource.product_parser`

        """
        try:
            assert line
        except AssertionError as e:
            raise ParserError("Invalid line.") from e

        fields = line.strip().split('::')

        try:
            # there are 3 fields in the movielens products line
            assert len(fields) == 3
        except AssertionError as e:
            raise ParserError("Unable to find 3 fields in the line.") from e

        try:
            int(fields[0])
        except ValueError as e:
            raise ParserError("Product id should be an integer.") from e

        return {
            self.product_col: int(fields[0]),
            'name': fields[1],
            'desc': fields[2]
        }

    @staticmethod
    def _validate_name(name):
        """ check for any unwanted characters in the given name """
        unwanted_chars = (' ', '-')

        for char in unwanted_chars:
            if char in name:
                message = "unwanted char {} in source name {}".format(char,
                                                                      name)
                logger.error(message)
                raise AssertionError(message)

    @staticmethod
    def _get_type(timestamp: int) -> str:
        """ hash function to classify a line as one of `training`, `test` or
        `validation`.

        """
        timestamp_hash = timestamp % 10

        if timestamp_hash < 6:
            data_type = "training"
        elif 6 <= timestamp_hash < 8:
            data_type = "validation"
        else:
            data_type = "test"

        return data_type
