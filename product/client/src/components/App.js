import React, { Component } from 'react';
import ProductCarousel from './product_carousel'
import RecommendationsCarousel from './recommendations_carousel'
import '../css/App.css';
import TopNav from "./top_nav";

class App extends Component {
  render() {
    return (
      <div className="App">

        <TopNav name={"movie"}/>

        <h1> Rate some movies! </h1>

        <ProductCarousel/>

        <h1> Your Recommendations </h1>

        <RecommendationsCarousel />

      </div>
    );
  }
}

export default App;

