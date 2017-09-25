# -*- coding: utf-8 -*-


class ParserError(Exception):
    """ Raised when a source parser fails. """
    pass


class WarehouseException(Exception):
    """ Raised when the warehouse encounters some internal error. """
    pass
