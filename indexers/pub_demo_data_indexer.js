'use strict';

const _ = require('lodash');
const bluebird = require('bluebird');

const uploadImages = require('./pub_images_uploader');
const elasticsearch = require('elasticsearch');

const elasticClient = new elasticsearch.Client({
  host: 'http://localhost:9200'
});

const categories = require('../categories.json');

const categoryMap = [
  {match: {category: 'AleStout'}, mapTo: '414'},
  {match: {category: 'Lager'}, mapTo: '414'},
  {match: {category: 'FinestCask'}, mapTo: '414'},
  {match: {category: 'Wine'}, mapTo: '421'},
  {match: {category: 'Spirits', subcategory: 'Liqueurs'}, mapTo: '2933'},
  {match: {category: 'Spirits', subcategory: 'Vodka'}, mapTo: '2107'},
  {match: {category: 'Spirits', subcategory: 'Brandy'}, mapTo: '2364'},
  {match: {category: 'Spirits', subcategory: 'Sambuca'}, mapTo: '2933'},
  {match: {category: 'Spirits', subcategory: 'White Rum'}, mapTo: '2605'},
  {match: {category: 'Spirits', subcategory: 'Dark and Golden Rum'}, mapTo: '2605'},
  {match: {category: 'Spirits', subcategory: 'Dark & Golden Rum'}, mapTo: '2605'},
  {match: {category: 'Spirits', subcategory: 'Blended Whisky'}, mapTo: '1926'},
  {match: {category: 'Spirits', subcategory: 'Malt Whisky'}, mapTo: '1926'},
  {match: {category: 'Spirits', subcategory: 'Imported Whisky'}, mapTo: '1926'},
  {match: {category: 'Spirits', subcategory: 'Gin'}, mapTo: '1671'},
  {match: {category: 'SoftDrinks', subcategory: 'Energy Drinks'}, mapTo: '5723'},
  {match: {category: 'SoftDrinks', subcategory: 'Juices'}, mapTo: '2887'},
  {match: {category: 'SoftDrinks', subcategory: 'Mixers'}, mapTo: '5725'},
  {match: {category: 'SoftDrinks', subcategory: 'Water'}, mapTo: '420'},
  {match: {category: 'SoftDrinks', subcategory: 'Cordials'}, mapTo: '8036'},
  {match: {category: 'SoftDrinks', subcategory: 'Canned Drinks'}, mapTo: '2628'},
  {match: {category: 'Cider'}, mapTo: '6761'},
  {match: {category: 'POS'}, mapTo: '5863'},
  {match: {category: 'ReadyToDrink'}, mapTo: '5887'},
  {match: {category: 'Sundries', subcategory: 'Co2'}, mapTo: '502975'},
  {match: {category: 'Sundries', subcategory: 'Cleaning Products'}, mapTo: '4973'}
];

const buildProduct = (source, id, priceResults) => {
  const product = _.pick(source, ['name', 'brand', 'description', 'long_description']);

  const categoryMapper = _.find(categoryMap, mapper => _.matches(mapper.match)(source));
  const category = categoryMapper ? categories[categoryMapper.mapTo] : {};
  product.category_code = category.hierachy;
  product.category_desc = category.name;

  const mapToOtherBrand = !product.brand || product.brand === 'Other Brands' || product.brand.indexOf('Finest Cask Rotation') === 0;
  product.brand = mapToOtherBrand ? 'Other' : product.brand;

  product.thumbnail_image_url = `https://res.cloudinary.com/dc3gcqic2/image/upload/${id}_A_p.jpg`

  const priceResponse = _.find(priceResults.responses, response => {
    return _.find(response.hits.hits, hit => {
      return hit._source.productid === id;
    });
  });
  product.price = priceResponse.hits.hits[0]._source.customerlistprice;
  product.was_price = null;

  return product;
};

let scrolling = false;
let processed = 0;
let total = 0;

const getPrices = productIds => {
  const priceQueries = productIds.map(id => {
    return {
      query: {
        term: {productid: id}
      },
      size: 1
    }
  });

  const searches = [];

  priceQueries.forEach(query => {
    searches.push({index: 'demo', _type: 'price'}, query);
  });

  return elasticClient.msearch({body: searches});
};

module.exports = function(productsIndex, suggestionsIndex) {
  return new Promise((resolve, reject) => {
    elasticClient.search({
      index: 'demo',
      scroll: '30s',
      search_type: 'scan',
      type: ['product'],
      body: {
        "query": {
          "bool": {
            "must": [
              {
                "has_child": {
                  "type": "price",
                  "query": {
                    "match_all": {}
                  }
                }
              }
            ],
            "must_not": [
              {
                "terms": {
                  "brand.raw": [
                    ".",
                    "Delivery Charge (Fri Same Day)",
                    "Delivery Charge (Off Day)",
                    "VAT"
                  ]
                }
              }
            ]
          }
        }
      }
    }, function getMoreUntilDone(err, response) {
      if (err) {
        console.error('Elasticsearch retrieval error.');
        return reject(err);
      }

      let result = Promise.resolve({});

      if (response.hits.hits.length > 0) {
        result = bluebird.filter(response.hits.hits, product => {
          return uploadImages.productHasImage(product._id).then(() => true, () => false);
        })
        .then(products => {
          if (products.length === 0) {
            return console.log(`Batch skipped due to missing product images.`);
          }

          return getPrices(_.map(products, '_id'))
            .then(priceResults => {
              const productIndexingRequests = [];
              const productIds = [];

              products
                .map(p => ({id: p._id, body: buildProduct(p._source, p._id, priceResults)}))
                  .filter(p => p.body.category_code)
                  .forEach(product => {
                    for(let supplier of [{name: 'Pub Taverns', idPrefix: 'p'}, {name: 'Beer & Wine Co', idPrefix: 'b'}]) {
                      const supplierProduct = _.clone(product.body);
                      supplierProduct.supplier = supplier.name;

                      // introduce small price variance
                      const adjustPrice = Math.random() > 0.5;
                      const adjustedPrice = adjustPrice ? supplierProduct.price * 0.98 : supplierProduct.price;

                      // ensure we only have 2 decimal places
                      supplierProduct.price = Math.round(adjustedPrice * 100) / 100;

                      productIndexingRequests.push({action: 'upsert', body: supplierProduct, objectID: `${supplier.idPrefix}${product.id}`});
                    }
                    productIds.push(product.id);
                  });

              return Promise.all([
                productsIndex.indexProductBatch(productIndexingRequests),
                suggestionsIndex.indexProductBatch(_.map(productIndexingRequests, 'body')),
                uploadImages(productIds)
              ]);
            }).then(_.spread((productsResponse, suggestionsResponse) => {
              if (productsResponse.errors) {
                console.error('Product indexing error.');
                return reject(productsResponse.errors);
              }
              if (suggestionsResponse.errors) {
                console.error('Suggestions indexing error.');
                return reject(suggestionsResponse.errors);
              }

              processed += response.hits.hits.length;
              console.log(`Indexed ${processed}/${total} products from Elasticsearch.`)
            }))
            .catch(err => {
              console.error('Batch indexing error.');
              return reject(err);
            });
        });
      }
      else if (scrolling) {
        console.log('Completed indexing from Elasticsearch');
        return resolve();
      }
      else {
        total = response.hits.total;
        console.log(`${total} products to index...`);
      }

      scrolling = true;

      return result.then(() => {
        return elasticClient.scroll({
          scrollId: response._scroll_id,
          scroll: '30s'
        }, getMoreUntilDone);
      })
      .catch(err => {
        console.error('Elasticsearch indexing error');
        reject(err);
      });
    });
  });
};
