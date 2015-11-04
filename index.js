'use strict';

const _ = require('lodash');

const ProductsIndex = require('./products_index');
const SuggestionsIndex = require('./suggestions_index');
const pubDemoDataIndexer = require('./indexers/pub_demo_data_indexer');

const apiBaseUrl = 'http://10.35.65.188:32768';

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
  console.log('Indexing pub demo data');
  return pubDemoDataIndexer(productsIndex, suggestionsIndex);
})
.catch(err => {
  console.error('Error. Aborting.');
  console.error(err);
  process.exit(1);
});
