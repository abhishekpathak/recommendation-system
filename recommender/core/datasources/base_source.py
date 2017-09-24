# -*- coding: utf-8 -*-

import logging
from abc import ABC, abstractmethod

from core import config

logger = logging.getLogger(__name__)


class BaseSource(ABC):

    def __init__(self, name, ratings_file, products_file, encoding):
        self.name = name

        self.ratings_file = ratings_file

        self.products_file = products_file

        self.encoding = encoding

        self.user_col = config.USER_COL

        self.product_col = config.PRODUCT_COL

        self.ratings_col = config.RATINGS_COL

    @abstractmethod
    def ratings_parser(self, line: str) -> dict:
        pass

    @abstractmethod
    def product_parser(self, line: str) -> dict:
        pass
