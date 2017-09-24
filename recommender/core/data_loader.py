# -*- coding: utf-8 -*-

import logging
from abc import abstractmethod, ABC

from core import config
from core.datasources.base_source import BaseSource
from core.exceptions import ParserError
from core.models import Products
from core.warehouse import FileWarehouse

logger = logging.getLogger(__name__)


class BaseDataLoader(ABC):
    @abstractmethod
    def create_ratings_data_in_warehouse(self):
        pass

    @abstractmethod
    def create_product_catalog_in_warehouse(self):
        pass

    @abstractmethod
    def create_ratings_data_in_serving_layer(self):
        pass

    @abstractmethod
    def create_product_catalog_in_serving_layer(self):
        pass


class DataLoader(BaseDataLoader):

    products_model = Products

    def __init__(self, source: BaseSource, warehouse: FileWarehouse) -> None:
        self.source = source
        self.warehouse = warehouse

    def create_ratings_data_in_warehouse(self, continue_on_error=False) -> None:
        files = self._get_warehouse_rating_file_handles()

        with open(self.source.ratings_file,
                  encoding=self.source.encoding) as source_ratings_file:
            for line in (next(source_ratings_file) for _ in range(10000)):
            # for line in source_ratings_file:

                try:
                    data = self.source.ratings_parser(line)
                    logger.debug('output from ratings parser :{}'.format(data))
                except ParserError as e:
                    logger.error(
                        "parsing error in line: {} of source file. "
                        "Error reported: {}".format(line, e))
                    if continue_on_error:
                        continue
                    else:
                        raise

                files_to_write = (files[data['metadata']['type']],
                                  files['ratings'])

                for file in files_to_write:
                    self.warehouse.write_row(file, data['payload'])

        for file in files.keys():
            files[file].close()

    def create_product_catalog_in_warehouse(self) -> None:
        with open(self.warehouse.products_file, 'w') as products_file:

            with open(self.source.products_file,
                      encoding=self.source.encoding) as source_products_file:
                for line in source_products_file:

                    try:
                        data = self.source.product_parser(line)
                        logger.debug(
                            'output from products parser :{}'.format(data))
                    except ParserError as e:
                        logger.error(
                            "parsing error in line: {} of source file. "
                            "Error reported: {}".format(line, e))
                        raise

                    self.warehouse.write_row(products_file, data)

    def create_ratings_data_in_serving_layer(self,
                                             continue_on_error=False) -> None:
        raise NotImplementedError("no support for loading historical"
                                  " ratings to serving layer currently.")

    def create_product_catalog_in_serving_layer(self) -> None:
        with open(self.source.products_file,
                  encoding=self.source.encoding) as source_products_file:
            for line in source_products_file:

                try:
                    data = self.source.product_parser(line)
                    logger.debug('output from products parser :{}'.format(data))
                except ParserError as e:
                    logger.error(
                        "parsing error in line: {} of source file. "
                        "Error reported: {}".format(line, e))
                    raise

                self.products_model.upsert(data_partition=self.source.name,
                                id=data[config.PRODUCT_COL],
                                name=data['name'],
                                desc=data['desc'])

    def _get_warehouse_rating_file_handles(self) -> dict:
        file_handles = {
            'ratings': open(self.warehouse.ratings_file, 'w'),
            'training': open(self.warehouse.training_file, 'w'),
            'test': open(self.warehouse.test_file, 'w'),
            'validation': open(self.warehouse.validation_file, 'w'),
        }

        return file_handles
