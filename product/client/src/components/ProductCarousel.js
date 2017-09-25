import React, { Component } from 'react';
import Product from './Product';
import Slider from 'react-slick';
import Rating from './Rating';
import 'react-rater/lib/react-rater.css';
import { config } from '../config';

class ProductCarousel extends Component {
  constructor() {
    super();

    this.state = {
      limit: 50,
      userId: 10001,
      data: [],
    };

    this.getProducts = this.getProducts.bind(this);
    this.submitRating = this.submitRating.bind(this);
  };

  componentDidMount() {
    this.getProducts();
  }

  /**
   * Fetches the products catalog from the API
   */
  getProducts() {
    let getProductsUri = encodeURI(config.SERVER_URL
                                  + '/products?limit='
                                  + this.state.limit
                                  + '&user_id='
                                  + this.state.userId);
    fetch(getProductsUri).then(response => response.json()).then(data => {
        this.setState({ data: data.products });
      });
  }

  /**
   * Submits a user rating to the API
   * @param {int} productId - the product id.
   * @param {int} rating - the rating, a value between 1 and 5.
   */
  submitRating(productId, rating) {
    let payload = {
      ratings: [
        {
          product_id: productId,
          rating: rating,
        },
      ],
    };

    let submitRatingsURL = encodeURI(config.SERVER_URL
                                    + '/users/'
                                    + this.state.userId
                                    + '/ratings');

    fetch(submitRatingsURL, {
      method: 'PUT',
      headers: new Headers(
        { 'Content-Type': 'application/json', Accept: 'application/json' }
      ),
      body: JSON.stringify(payload),
    }).then((response) => {
      if (!response.ok) {
        console.log('failed to submit rating.'
                    + 'The ratings API returned HTTP code: '
                    + response.status);
      }
    });
  }

  render() {
    const carouselSettings = {
      dots: true,
      arrows: true,
      infinite: false,
      speed: 1500,
      slidesToShow: 5,
      slidesToScroll: 5,
      variableWidth: true,
    };

    let products = <div><h5>loading...</h5></div>;

    if (this.state.data.length !== 0) {
      products = this.state.data.map((element, index) =>
        <div key={element.product_id}>
          <Product id={element.product_id}
                   name={element.meta.product_name}
                   desc={element.meta.product_desc}
                   rating={element.rating} />
          <Rating id={element.product_id}
                  rating={element.rating}
                  afterClick={this.submitRating} />
        </div>
      );
    }

    return (
      <div>
        <Slider {...carouselSettings}>
          {products}
        </Slider>
      </div>
    );
  }
}

export default ProductCarousel;
