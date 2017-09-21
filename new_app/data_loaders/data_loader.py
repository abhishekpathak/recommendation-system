# -*- coding: utf-8 -*-
import json
from abc import ABC, abstractmethod
import logging
from io import StringIO

from new_app.exceptions import ParserError
from new_app.source import Source
from new_app.warehouse_handler import Warehouse

logger = logging.getLogger(__name__)


class DataLoader(ABC):

    @abstractmethod
    def create_ratings_data(self):
        pass

    @abstractmethod
    def create_product_catalog(self):
        pass


class MovieLensDataLoader(DataLoader):

    def __init__(self, source: Source) -> None:
        self.source = source
        self.warehouse = Warehouse(source=self.source)

    def create_ratings_data(self, continue_on_error=False) -> None:
        files = self._get_file_handles()

        with open(self.source.ratings_file, encoding=self.source.encoding) as source_ratings_file:
                for line in source_ratings_file:

                    try:
                        data = self._ratings_parser(line)
                        logger.debug('output from ratings parser :{}'.format(data))
                    except ParserError as e:
                        logger.error("parsing error in line: {} of source file. Error reported: {}".format(line, e))
                        if continue_on_error:
                            continue
                        else:
                            raise

                    files_to_write = (data['metadata']['type'], 'ratings')

                    for file in files_to_write:
                        self._write(file, data['payload'])

        for file in files.keys(): files[file].close()

    def create_product_catalog(self) -> None:
        files = self._get_file_handles()

        with open(self.source.products_file, encoding=self.source.encoding) as source_products_file:
                for line in source_products_file:

                    try:
                        data = self._product_parser(line)
                        logger.debug('output from products parser :{}'.format(data))
                    except ParserError as e:
                        logger.error("parsing error in line: {} of source file. Error reported: {}".format(line, e))
                        raise

                    files_to_write = (data['metadata']['type'], 'ratings')

                    for file in files_to_write:
                        self._write(data['payload'], file)

        for file in files.keys(): files[file].close()

    @staticmethod
    def _ratings_parser(line: str) -> dict:
        try:
            assert line
        except AssertionError:
            raise ParserError()

        fields = line.strip().split("::")

        try:
            assert len(fields) == 4
        except AssertionError:
            raise ParserError()

        timestamp_hash = int(fields[3]) % 10

        if timestamp_hash < 6:
            data_type = "training"
        elif 6 <= timestamp_hash < 8:
            data_type = "validation"
        else:
            data_type = "test"

        return {
            'type': data_type,
            'data': {
                'user_id': int(fields[0]),
                'product_id': int(fields[1]),
                'rating': float(fields[2])
            }
        }

    @staticmethod
    def _product_parser(line: str) -> dict:
        try:
            assert line
        except AssertionError:
            raise ParserError()

        fields = line.strip().split('::')
        try:
            assert len(fields) == 3
        except AssertionError:
            raise ParserError()

        return {
            'id': int(fields[0]),
            'name': fields[1],
            'desc': fields[2]
        }

    def _get_file_handles(self):
        file_handles = {
            'ratings': open(self.warehouse.ratings_file, 'w'),
            'training': open(self.warehouse.training_file, 'w'),
            'test': open(self.warehouse.test_file, 'w'),
            'validation': open(self.warehouse.validation_file, 'w'),
            'products': open(self.warehouse.products_file, 'w')
            }

        return file_handles

    @staticmethod
    def _write(file_handle: StringIO, data: dict) -> None:
        json.dump(data, file_handle)
        file_handle.write('\n')
