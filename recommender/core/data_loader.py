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
    """ Documents the contract that a data loader should follow.

    A data loader is responsible for taking an external data source, like the
    movielens dataset, and populating it into the system in a structured
    manner. It will typically populate the data in the warehouse for the
    engine to pick, and in the serving database for the product UI to pick.

    """
    @abstractmethod
    def create_ratings_data_in_warehouse(self, **kwargs) -> None:
        """ loads the ratings data store in the warehouse.

        Args:
            **kwargs: Arbitrary keyword arguments.

        """
        pass

    @abstractmethod
    def create_product_catalog_in_warehouse(self, **kwargs) -> None:
        """ loads the product details in the warehouse.

        Args:
            **kwargs: Arbitrary keyword arguments.

        """
        pass

    @abstractmethod
    def create_ratings_data_in_serving_layer(self, **kwargs) -> None:
        """ loads the ratings data store in the serving db.

        Args:
            **kwargs: Arbitrary keyword arguments.

        """
        pass

    @abstractmethod
    def create_product_catalog_in_serving_layer(self, **kwargs) -> None:
        """ loads the product details in the serving db.

        Args:
            **kwargs: Arbitrary keyword arguments.

        """
        pass


class DataLoader(BaseDataLoader):
    """ An implementation of `BaseDataLoader`.

    The data loader has a handle to its data source, and uses its API to parse
    and categorize the external dataset. It can work with any data source
    implementing the base source contract.

    The data loader also has a handle to the warehouse and the product models,
    and uses their APIs to populate data.

    Attributes:
        source: the data source (contains all details about the external data)

        warehouse: the warehouse instance (to reason about the internal data
        structures)

    """

    # better set a class-level instance instead of using the global.
    # This facilitates unit testing by IoC.
    products_model = Products

    def __init__(self, source: BaseSource, warehouse: FileWarehouse) -> None:
        self.source = source
        self.warehouse = warehouse

    def create_ratings_data_in_warehouse(self, continue_on_error: bool = False) -> None:
        """ Populates the various ratings files (ratings, training, test,
        validation) in the warehouse.

        Args:
            continue_on_error: should an error in loading one row abort the
            entire process, or let it continue?

        """
        files = self._get_warehouse_rating_file_handles()

        with open(self.source.ratings_file,
                  encoding=self.source.encoding) as source_ratings_file:

            for line in source_ratings_file:

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

                # write the line to the ratings file, and one of
                # the training, test or validation files.
                # the logic for choosing which of the latter 3 files
                # should ideally be with the data loader.
                # For convenience, here type comes from
                # the source parser and the loader uses it directly.
                files_to_write = (files['ratings'],
                                  files[data['metadata']['type']])

                for file in files_to_write:
                    self.warehouse.write_row(file, data['payload'])

        for file in files.keys():
            files[file].close()

    def create_product_catalog_in_warehouse(self) -> None:
        """ Populates the products file in the warehouse. """
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

    def create_ratings_data_in_serving_layer(self, continue_on_error=False) -> None:
        """ Populate the ratings data to the serving db.

        This method is not implemented for this system. We are not interested in
        serving up the ratings for any of the historical users.

        """
        raise NotImplementedError("no support for loading historical"
                                  " ratings to serving layer currently.")

    def create_product_catalog_in_serving_layer(self) -> None:
        """ Populates the products table in the serving db. """
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
        """ return file handles to all the ratings files in the warehouse """
        file_handles = {
            'ratings': open(self.warehouse.ratings_file, 'w'),
            'training': open(self.warehouse.training_file, 'w'),
            'test': open(self.warehouse.test_file, 'w'),
            'validation': open(self.warehouse.validation_file, 'w'),
        }

        return file_handles
