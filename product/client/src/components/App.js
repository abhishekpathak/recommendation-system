import React, { Component } from 'react';
import ProductCarousel from './ProductCarousel'
import RecommendationsCarousel from './RecommendationCarousel'
import '../css/App.css';
import TopNav from "./TopNavBar";

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

