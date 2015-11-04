'use strict';

const fs = require('fs');

const _ = require('lodash');
const request = require('request-promise');

const ProductsIndex = require('./products_index');
const SuggestionsIndex = require('./suggestions_index');
const pubDemoDataIndexer = require('./indexers/pub_demo_data_indexer');
const bestBuyIndexer = require('./indexers/best_buy_indexer');
const wallmartIndexer = require('./indexers/wallmart_indexer');

const apiBaseUrl = 'https://api.bigwednesday.io/1/search';

const productsIndex = new ProductsIndex(apiBaseUrl, '8N*b3i[EX[s*zQ%');
const suggestionsIndex = new SuggestionsIndex(apiBaseUrl, '8N*b3i[EX[s*zQ%');

console.log('Deleting existing indexes');

Promise.all([
  suggestionsIndex.delete(),
  productsIndex.delete()
])
.then(() => {
  console.log('Recreating indexes');

  return Promise.all([
    suggestionsIndex.create(),
    productsIndex.create()
  ]);
})
.then(() => {
  console.log('Getting categories');
  return request({uri: 'https://raw.githubusercontent.com/BigWednesdayIO/categories-api/master/categories.json', json: true})
            .then(categories => {
              fs.writeFileSync('./categories.json', JSON.stringify(categories));
            });
})
.then(() => {
  console.log('Indexing pub demo data');
  return pubDemoDataIndexer(productsIndex, suggestionsIndex);
})
.then(() => {
  console.log('Indexing Best Buy data');
  return bestBuyIndexer(productsIndex);
})
.then(() => {
  console.log('Indexing Wallmart data');
  return wallmartIndexer(productsIndex);
})
.catch(err => {
  console.error('Error. Aborting.');
  console.error(err);
  process.exit(1);
});
