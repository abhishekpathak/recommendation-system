import React, { Component } from 'react';
import Product from './Product';
import Slider from 'react-slick';
import { config } from '../config';

class RecommendationsCarousel extends Component {

  constructor() {
    super();

    this.state = {
      userId: 10001,
      data: [],
    };

    this.getRecommendations = this.getRecommendations.bind(this);
  }

  componentDidMount() {
    this.getRecommendations();
  }

  /**
   * fetches recommendations from the API.
   */
  getRecommendations() {
    let getRecommendationsURL = encodeURI(config.SERVER_URL
                                          + '/users/'
                                          + this.state.userId
                                          + '/recommendations');
    fetch(getRecommendationsURL).then(response => response.json()).then((data) => {
      this.setState({ data: data.recommendations });
    });
  }

  render() {
    const settings = {
      dots: true,
      arrows: true,
      infinite: false,
      speed: 500,
      slidesToShow: 5,
      variableWidth: true,
    };

    let recommendations = <div><h5>loading...</h5></div>;

    if (this.state.data.length !== 0) {
      recommendations = this.state.data.map((element, index) =>
        <div key={element.product_id}>
          <Product id={element.product_id}
                   name={element.meta.name}
                   desc={element.meta.desc} />
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
