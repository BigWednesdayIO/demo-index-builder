'use strict';

const request = require('request-promise');
const _ = require('lodash');

const ProductsIndex = require('./products_index');
const productsIndex = new ProductsIndex('https://api.bigwednesday.io/1/search', '8N*b3i[EX[s*zQ%');

const wallmartCategories = ['1229749_1086977_1086987', '1229749_1086977_1086990'];
const apiKey = 'zvu7f5zhqckaecazm3765jjw';
const walmartBaseUrl = 'http://api.walmartlabs.com';

const categories = require('./categories.json');
const categoryMappers = [{
  match: {categoryNode: '1229749_1086977_1086987'},
  mapsTo: '4973',
}, {
  match: {categoryNode: '1229749_1086977_1086990'},
  mapsTo: '2530',
}];

const buildProduct = source => {
  const categoryMapper = _.find(categoryMappers, mapper => _.matches(mapper.match)(source));
  const category = categoryMapper ? categories[categoryMapper.mapsTo] : {};

  return {
    name: source.name,
    brand: source.brandName,
    description: source.shortDescription,
    long_description: source.longDescription,
    supplier: 'Wallmart',
    category_code: category.hierachy,
    category_desc: category.description
  }
};

const addBatchToIndex = response => {
  if (response && response.items) {
    const requests = response.items
                      .map(sourceProduct => ({
                        action: 'upsert',
                        body: buildProduct(sourceProduct),
                        objectID: sourceProduct.itemId.toString()
                      }));

    return productsIndex
            .indexProductBatch(requests)
            .then(() => {
              console.log(`Indexed ${response.items.length} wallmart products`);
            });
  }
}

const indexItems = function indexItems (nextPageUri) {
  if (!nextPageUri) {
    return;
  }

  const itemsPromise = request({uri: `${walmartBaseUrl}${nextPageUri}`, json: true});

  return Promise.all([
    itemsPromise.then(addBatchToIndex),
    itemsPromise.then(response => response.nextPage).then(indexItems)
  ])
  .catch(err => {
    console.error(err);
    process.exit(1);
  });
}

// Indexes categories concurrently
wallmartCategories.forEach(categoryId => {
  indexItems(`/v1/paginated/items?category=${categoryId}&apiKey=${apiKey}`)
    .then(() => {
      console.log(`Indexing of Wallmart products in category ${categoryId} complete`);
    });
});
