import React, { Component } from 'react';

class Product extends Component {

  render() {
    return (
      <div className="product">
        <pre>{this.props.name}</pre>
        <pre>{this.props.desc}</pre>
      </div>
    );
  }
}

export default Product;
