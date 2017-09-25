import React, { Component } from 'react';

import Rater from 'react-rater';
import 'react-rater/lib/react-rater.css';

class Rating extends Component {

  constructor(props) {
    super(props);

    this.state = {
      id: this.props.id,
      total: 5,
      rating: this.props.rating,
    };

    this.handleClick = this.handleClick.bind(this);
  }

  /**
   * Saves the rating to state on clicking.
   * @param event - a React Synthetic click event.
   */
  handleClick(event) {
    if (event.type === 'click') {
      this.props.afterClick(this.state.id, event.rating);
    }
  }

  render() {
    return (
      <Rater id={this.state.id}
             total={this.state.total}
             rating={this.state.rating}
             onRate={this.handleClick} />
    );
  }
}

export default Rating;
