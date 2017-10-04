[![Build Status](https://travis-ci.org/abhishekpathak/recommendation-system.svg?branch=master)](https://travis-ci.org/abhishekpathak/recommendation-system)

# Recommendation System

A simple ALS-based recommendation system working for a dummy SaaS product. This example takes the [Movielens 1M dataset](https://grouplens.org/datasets/movielens/1m/) and makes a movie recommender ([screenshot](/extras/screenshot_product.jpg)). The datasets are pluggable, so this system can be extended to any product.

## System design

![System design](/extras/design.jpg?raw=true)

## Project structure

* **Recommender** (offline)
  * **core** - consists of the engine and other core components.
  * **server** - application services that expose the core API over REST.
  * **client** - consumes the server APIs and interacts with an admin user.
  
* **Product** (online)
  * **server** - application services doing CRUD to serving database (and interacting with other services if needed).
  * **client** - consumes the server APIs and interacts with a consumer.
  
  There is no coupling between Recommender and Product. In addition, all of their sub-components are independent services.
  
## Design choices/trade-offs
* **Fast but stale recommendations**. Generating recommendations is a slow task. To tackle that, the recommendations are generated offline periodically and stored. While this provides faster response times to the user, the recommendations will not reflect the user's real-time actions. 

    There are a couple of ways to solve for this:
    * **Design the engine to provide real-time recommendations**. Not sure how to do that, since the model needs to be retrained on real-time data before reflecting that in its output, and training is again a slow process. Maybe the model can train on only the new data?  
    * **Have other services/engines complement the ALS
    recommendations** with other real-time recommendations. For example: including more products by the same vendor, or including best-sellers in the same category.
* **Loose coupling** - provides flexibility as each component can be developed and scaled independently. However, this can lead to a lot of chattiness between systems. If the messages need to be passed over a network, the system will suffer from all network-based concerns - latency, reliability etc.

## Coding assumptions
* **The transport layer** - This project uses a transporter object that directly interacts with data stores. In the real world, this would typically be a bunch of pipeline jobs that leverage a messaging system like Kafka.
* **The serving database** - This project uses redis (I was already using it as a message broker), with a quick-and-dirty ORM layer on top. However, a RDBMS is much more suited to the job.
* **The warehouse** - For this project, I've prototyped a warehouse which stores and processes files (each line a json) on the local file system. In the real world, this would typically be a distributed big data processing system such as Hadoop. 



 
