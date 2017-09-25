# -*- coding: utf-8 -*-

import logging
from abc import ABC, abstractmethod

from core import config

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """ An abstract base source.

    Multiple data sets can be added to the system. For
    integrating any new dataset, a new source class needs to be created.
    This class provides a contract for any such new source class.

    Attributes:
        name: An identifier of the data source.

        ratings_file: Path to a file where each line contains a rating given
        to a product by a user.

        products_file: Path to a file where each line contains a product,
        with optional metadata.

        encoding: The encoding to be used for reading the ratings and
        product files.

        user_col(string): the key to be used to look up a user id.
        eg: 'user_id'

        product_col(string): the key to be used to look up a product id.
        eg: 'product_id'

        ratings_col(string): the key to be used to look up a rating.
        eg: 'rating'

    """

    def __init__(self, name: str, ratings_file: str, products_file: str,
                 encoding: str):
        self.name = name

        self.ratings_file = ratings_file

        self.products_file = products_file

        self.encoding = encoding

        self.user_col = config.USER_COL

        self.product_col = config.PRODUCT_COL

        self.ratings_col = config.RATINGS_COL

    @abstractmethod
    def ratings_parser(self, line: str) -> dict:
        """ A method which can parse a line of the ratings file. Raises a
        `ParserError` in case of any issue.

        Args:
            line: a line from the ratings file

        Returns:
            A dict with two keys : `metadata` and `payload`.

            `metadata` can one of 3 values: 'training`, `test` and `validation`.
            This will be used during the training of the engine.

            `payload` is again a dict. The keys are the strings associated with
            `user_col`, `product_col` and `ratings_col`. The values are the
            user id, product id and rating respectively. The values must be
            integers for the ALS engine to work.

            It is the responsibility of the parser to hash each line so that it
            falls into one of the metadata values.

        """
        pass

    @abstractmethod
    def product_parser(self, line: str) -> dict:
        """ A method which can parse a line of the products file. Raises a
        `ParserError` in case of any issue.

        Args:
            line: a line from the products file

        Returns:
            A dict. The keys are the product id, product name and product
            description. The product id should be an int. The values of the
            latter two can be empty strings.

        """
        pass
