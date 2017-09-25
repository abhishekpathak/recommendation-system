import React, {Component} from 'react';
import Product from './Product';
import Slider from 'react-slick';
import {config } from '../config.js';

class RecommendationsCarousel extends Component {

    constructor() {
        super();

        this.state = {
            user_id: 10001,
            data: []
        };

        this.getProducts = this.getProducts.bind(this);
    }

    componentDidMount() {
        this.getProducts();
    }

    getProducts() {
        let get_recommendations_uri = encodeURI(config.SERVER_URL + "/users/" + this.state.user_id + "/recommendations")
        fetch(get_recommendations_uri).then((response) => {
            return response.json();
        }).then((data) => {
            this.setState({data: data['recommendations']});
        });
    }

    render() {
        const settings = {
            dots: true,
            arrows: true,
            infinite: false,
            speed: 500,
            slidesToShow: 5,
            variableWidth: true
        };

        let recommendations = <div><h5>loading...</h5></div>;

        if (this.state.data.length !== 0) {
            recommendations = this.state.data.map((element, index) =>
                <div key={element.product_id}>
                    <Product id={element.product_id}
                             name={element.meta.name}
                             desc={element.meta.desc}
                    />
                </div>
            );
        }

        return (
            <Slider {...settings}>
                {recommendations}
            </Slider>
        );
    }
}

export default RecommendationsCarousel;