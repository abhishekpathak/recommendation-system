# -*- coding: utf-8 -*-
import json
import logging

logger = logging.getLogger(__name__)


def deserialize_json_list(data: list) -> list:
    if data:
        return [json.loads(item) for item in data]
    else:
        return []
