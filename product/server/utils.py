# -*- coding: utf-8 -*-
import json


def deserialize_json_list(data: list) -> list:
    """ transforms a list of jsons to a list of native types """
    if data:
        return [json.loads(item) for item in data]
    else:
        return []
