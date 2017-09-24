# -*- coding: utf-8 -*-

import logging

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from server import api
from server.exceptions import HTTPBadRequest
from server.models import Users, Products

logger = logging.getLogger(__name__)


def _is_valid_user(user_id: int) -> bool:
    try:
        Users.get(user_id)
        return True
    except KeyError:
        return False


class RatingsResource(Resource):
    def get(self, user_id: int):
        try:
            assert _is_valid_user(user_id)
        except AssertionError:
            message = 'invalid user id:{}'.format(user_id)
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        user = Users.get(user_id)

        ratings = user.get_ratings()

        return {
            'ratings': ratings
        }

    def put(self, user_id: int):
        try:
            assert _is_valid_user(user_id)
        except AssertionError:
            message = 'invalid user id:{}'.format(user_id)
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        try:
            assert self._has_valid_ratings_data(request)
        except AssertionError:
            message = "invalid ratings data."
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        user = Users.get(user_id)

        ratings = request.get_json()['ratings']

        user.set_ratings(ratings)

        return {
            'message': 'success'
        }

    @staticmethod
    def _has_valid_ratings_data(request_obj) -> bool:
        try:
            _ = request_obj.get_json()['ratings']
            return True
        except (BadRequest, KeyError):
            return False


class RecommendationsResource(Resource):
    def get(self, user_id: int):

        try:
            _is_valid_user(user_id)
        except AssertionError:
            message = 'invalid user id:{}'.format(user_id)
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        user = Users.get(user_id)

        recommendations = self._get_recommendations(user)

        return {
            'recommendations': recommendations
        }

    def _get_recommendations(self, user: Users) -> list:
        all_recommendations = self._get_curated_or_default(user)

        filtered_recommendations = self._filter_recommendations(user,
                                                                all_recommendations)

        detailed_recommendations = []

        for item in filtered_recommendations:
            detailed_recommendations.append(self._get_details(item))

        return detailed_recommendations

    @staticmethod
    def _get_curated_or_default(user: Users) -> list:
        recommendations = user.get_recommendations()

        if not recommendations:
            logger.debug('No curated recommendations. Picking default ones.')
            recommendations = user.get_default_recommendations()

        logger.debug({' recommendations:{}'.format(recommendations)})

        return recommendations

    @staticmethod
    def _filter_recommendations(user: Users, all_: list) -> list:
        used = user.get_products_used()

        filtered = [item for item in all_ if item not in used]

        logger.debug('filtered recommendations:{}'.format(filtered))

        return filtered

    @staticmethod
    def _get_details(product_id: int) -> dict:
        product = Products.get(product_id)

        return {
            'product_id': product.id,
            'meta': {
                'name': product.name,
                'desc': product.desc
            }
        }


class ProductsResource(Resource):
    def get(self):

        offset = int(request.args.get('offset', 1))

        limit = int(request.args.get('limit', 50))

        user_id = request.args.get('user_id')

        if user_id:
            try:
                _is_valid_user(user_id)
            except AssertionError:
                message = 'invalid user id:{}'.format(user_id)
                logger.error(message)
                raise HTTPBadRequest(message, payload={'message': message})

        user = Users.get(id=int(user_id))

        products = self._get_products(user, offset, limit)

        return {
            'products': products,
            'next': api.url_for(ProductsResource, offset=offset + limit,
                                limit=limit, user_id=user_id)
        }

    def _get_products(self, user: Users, offset: int, limit: int) -> list:
        products = self._get_product_catalog(offset, limit)

        user_ratings = user.get_ratings()

        return self._transform_products(products, user_ratings)

    def _get_product_catalog(self, offset: int, limit: int) -> list:
        products = []

        for product_id in range(offset, offset + limit):
            product = Products.get(product_id)
            if self._is_valid(product):
                product_data = {
                    'product_id': product.id,
                    'meta': {
                        'product_name': product.name,
                        'product_desc': product.desc
                    },
                    'rating': -1
                }
                products.append(product_data)

        return products

    @staticmethod
    def _is_valid(product_detail):
        return product_detail is not None

    @staticmethod
    def _transform_products(products: list, user_ratings: list) -> list:
        transformed_products = []

        # TODO might be inefficient, think of a better way!
        for product_data in products:

            for user_rating in user_ratings:
                if user_rating['product_id'] == product_data['product_id']:
                    product_data['rating'] = user_rating['rating']
                    break

            transformed_products.append(product_data)

        return transformed_products
