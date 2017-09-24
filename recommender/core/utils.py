# -*- coding: utf-8 -*-
import json

import os
import shutil


def create_directory(directory: str) -> None:
    if not os.path.exists(directory):
        os.makedirs(directory)


def delete_directory(directory: str) -> None:
    if os.path.exists(directory):
        shutil.rmtree(directory)


def touch_file(file: str) -> None:
    open(file, 'w').close()
