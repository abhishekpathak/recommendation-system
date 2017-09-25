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
    """ module-level function to valid a user id.

    Args:
        user_id: the user id to validate.

    Returns:
        true if the user id is valid, false otherwise.

    """
    try:
        Users.get(user_id)
        return True
    except KeyError:
        return False


class RatingsResource(Resource):
    """ Exposes the ratings associated with a user id as a resource for REST.

    """
    def get(self, user_id: int):
        """ fetches the ratings submitted by a user.

        Args:
            user_id: the id of the user.

        Returns:
            a response object (either directly or implicitly done by Flask)

        """

        # validate the user id
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
        """ submits the ratings by a user to the system.

        Args:
             user_id: the id of the user.

        Returns:
            a response object (either directly or implicitly done by Flask)

        """

        # validate the user id
        try:
            assert _is_valid_user(user_id)
        except AssertionError:
            message = 'invalid user id:{}'.format(user_id)
            logger.error(message)
            raise HTTPBadRequest(message, payload={'message': message})

        # validate the payload recieved.
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
        """ validates the ratings payload """
        try:
            _ = request_obj.get_json()['ratings']
            return True
        except (BadRequest, KeyError):
            return False


class RecommendationsResource(Resource):
    """ Exposes the recommendations associated with a user as a resource for REST

    """
    def get(self, user_id: int):
        """ fetches the recommendations for a user

        Args:
            user_id: the id of the user

        Returns:
            a response object (either directly or implicitly done by Flask)

        """

        # validate the user id
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
        """ driver method to orchestrate the ratings extraction.

        This method determines if curated or a default recommendations need to
        be shown. It also filters the recommendations, and associates metadata
        with the product ids for final consumption by the client.

        The logic of this method is subjective - it can be as simple or as
        complex as needed by the product business considerations. It can mix
        and match the engine-based recommendations with other
        considerations, like showing sponsored products, drawing in from the
        recently used products, or recommending used products (eg. movies to
        watch again) etc.

        Args:
            user: a user object

        Returns:
            a list of final recommendations.

        """
        all_recommendations = self._get_curated_or_default(user)

        filtered_recommendations = self._filter_recommendations(user, all_recommendations)

        detailed_recommendations = []

        for item in filtered_recommendations:
            detailed_recommendations.append(self._get_details(item))

        return detailed_recommendations

    @staticmethod
    def _get_curated_or_default(user: Users) -> list:
        """ gets the curated or default recommendations as the case may be.

        Args:
            user: the user object

        Returns:
            a list of recommendations (curated/default)

        """
        recommendations = user.get_recommendations()

        if not recommendations:
            logger.debug('No curated recommendations. Picking default ones.')
            recommendations = user.get_default_recommendations()

        logger.debug({' recommendations:{}'.format(recommendations)})

        return recommendations

    @staticmethod
    def _filter_recommendations(user: Users, all_: list) -> list:
        """ helper method to weed out the used products from the
        recommendation pool.

        Args:
            user: the user object

        Returns:
            the filtered list of recommendations.

        """
        used = user.get_products_used()

        filtered = [item for item in all_ if item not in used]

        logger.debug('filtered recommendations:{}'.format(filtered))

        return filtered

    @staticmethod
    def _get_details(product_id: int) -> dict:
        """ queries the product catalog to associate metadata with a
        product id.

        Args:
            product_id: the product id

        Returns:
            a dict containing the product id and its metadata.
        """

        product = Products.get(product_id)

        return {
            'product_id': product.id,
            'meta': {
                'name': product.name,
                'desc': product.desc
            }
        }


class ProductsResource(Resource):
    """ Exposes the products as Resources for REST.

    """
    def get(self):
        """ Fetches the product catalog in a paginated fashion. The products
        are also decorated with user-specific ratings.

        Request args:
            user_id: id of the user

            offset : the offset from which to start fetching the product ids.

            limit: the number of products to fetch at a time.

        Returns:
            a response object (either directly or implicitly done by Flask)

        """
        offset = int(request.args.get('offset', 1))

        limit = int(request.args.get('limit', 50))

        user_id = request.args.get('user_id')

        # validate the user id
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
        """ driver method orchestrating and handling all transformations to
        the product catalog for final consumption by the client.

        Args:
            user: the user object

            offset : the offset from which to start fetching the product ids

            limit: the number of products to fetch

        Returns:
            a list of products. Each product is represented by a dict
            containing the product id, metadata and user-submitted rating on
            that product, if any.

        """
        products = self._get_product_catalog(offset, limit)

        user_ratings = user.get_ratings()

        return self._transform_products(products, user_ratings)

    def _get_product_catalog(self, offset: int, limit: int) -> list:
        """ fetches metadata for products.

        Args:
            offset : the offset from which to start fetching the product ids

            limit: the number of products to fetch.

        Returns:
            a list of products. Each product is represented by a dict
            containing the product id, metadata and a default rating of -1,
            signifying that the rating has not been decorated with user data
            yet.

        """
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
    def _is_valid(product):
        """ checks if a product is valid (for now this means checking if
        it has metadata)

        """
        return hasattr(product, 'name') and hasattr(product, 'desc')

    @staticmethod
    def _transform_products(products: list, user_ratings: list) -> list:
        """ transforms the products catalog to superimpose user ratings on top

        Args:
            products: a list of products, each product is a dict containing
            a product id, metadata and a default rating of -1.

            user_ratings: a list of user-submitted ratings, each rating is a
            dict with a product id and a rating.

        Returns:
            a list of patched products, with user ratings superimposed.
        """
        transformed_products = []

        # TODO inefficient, think of a better way!
        for product_data in products:

            for user_rating in user_ratings:

                if user_rating['product_id'] == product_data['product_id']:
                    product_data['rating'] = user_rating['rating']
                    break

            transformed_products.append(product_data)

        return transformed_products
