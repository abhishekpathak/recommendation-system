# -*- coding: utf-8 -*-
from server import api
from server.resources import RatingsResource, RecommendationsResource, \
    ProductsResource

api.add_resource(RatingsResource, '/api/v1/users/<int:user_id>/ratings')

api.add_resource(RecommendationsResource,
                 '/api/v1/users/<int:user_id>/recommendations')

api.add_resource(ProductsResource, '/api/v1/products')
