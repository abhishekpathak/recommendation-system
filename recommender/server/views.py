# -*- coding: utf-8 -*-
from server import api
from server.resources import EngineResource, EnginesResource, TaskResource

api.add_resource(EngineResource, '/engines/<engine_id>')

api.add_resource(EnginesResource, '/engines/')

api.add_resource(TaskResource, '/tasks/<task_id>')
