'use strict';

const request = require('request-promise');
const _ = require('lodash');

const categoryId = '976759';
const apiKey = 'zvu7f5zhqckaecazm3765jjw';
const walmartBaseUrl = 'http://api.walmartlabs.com';

const addBatchToIndex = response => {
  if (response && response.items) {
    console.log(`Indexed ${response.items.length} wallmart products`);
  }
  return response;
}

const indexItems = function indexItems (nextPageUri) {
  if (!nextPageUri) {
    return;
  }

  const itemsPromise = request({uri: `${walmartBaseUrl}${nextPageUri}`, json: true});

  return itemsPromise
    .then(addBatchToIndex)
    .then(response => {
      console.log(_.omit(response, 'items'));
      return response.nextPage
    })
    .then(indexItems)
    .catch(err => {
      console.error(err);
      process.exit(1);
    });
}

indexItems(`/v1/paginated/items?category=${categoryId}&apiKey=${apiKey}`)
  .then(() => {
    console.log('Indexing of Wallmart products in category ${categoryId} complete');
  });

