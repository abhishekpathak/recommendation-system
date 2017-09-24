# -*- coding: utf-8 -*-
import json

import os
import shutil


def deserialize_json_list(list):
    if list:
        return [json.loads(item) for item in list]
    else:
        return []


def _create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def _delete_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)


def _touch_file(file):
    open(file, 'w').close()