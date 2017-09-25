import React, {Component} from 'react';
import Product from './Product';
import Slider from 'react-slick';
import Rating from './Rating';
import 'react-rater/lib/react-rater.css';
import {config } from '../config.js';

class ProductCarousel extends Component {
    constructor() {
        super();

        this.state = {
            'limit': 50,
            'user_id': 10001,
            'data': []
        };

        this.getProducts = this.getProducts.bind(this);
        this.submitRating = this.submitRating.bind(this);
    };

    componentDidMount() {
        this.getProducts();
    }

    getProducts() {
        let get_products_uri = encodeURI(config.SERVER_URL + "/products?limit=" + this.state.limit + "&user_id=" + this.state.user_id)
        fetch(get_products_uri).then((response) => {
            return response.json();
        }).then((data) => {
            this.setState({data: data['products']});
        });
    }

    submitRating(product_id, rating) {
        let payload = {
            'ratings': [
                {
                    'product_id': product_id,
                    'rating': rating
                }
            ]
        };

        let submit_ratings_uri = encodeURI(config.SERVER_URL + "/users/" + this.state.user_id + "/ratings");

        fetch(submit_ratings_uri, {
            method: "PUT",
            headers: new Headers(
                {"Content-Type": "application/json", "Accept":"application/json"}
            ),
            body: JSON.stringify(payload)
        }).then((response) => {
          if (!response.ok) {
              console.log('failed to submit rating. The ratings API returned HTTP code ' + response.status);
          }
        });
    }

    render() {
        const carousel_settings = {
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
                             rating={element.rating}/>
                    <Rating id={element.product_id} rating={element.rating} afterClick={this.submitRating}/>
                </div>
            );
        }

        return (
            <div>
            <Slider {...carousel_settings}>
                {products}
            </Slider>
            </div>
            );
        }
}

export default ProductCarousel;