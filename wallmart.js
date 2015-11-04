'use strict';

const request = require('request-promise');
const _ = require('lodash');

const ProductsIndex = require('./products_index');
const productsIndex = new ProductsIndex('https://api.bigwednesday.io/1/search', '8N*b3i[EX[s*zQ%');

const wallmartCategories = ['1229749_1086977_1086987', '1229749_1086977_1086990'];
const apiKey = 'zvu7f5zhqckaecazm3765jjw';
const walmartBaseUrl = 'http://api.walmartlabs.com';

const categories = require('./categories.json');
const categoryMap = [
  {match: {categoryNode: '1229749_1086977_1086987'}, mapTo: '4973'},
  {match: {categoryNode: '1229749_1086977_1086990'}, mapTo: '2530'},
  {match: {categoryNode: '1115193_1073264_1149383'}, mapTo: '2530'},
  {match: {categoryNode: '1115193_1073264_1149384'}, mapTo: '629'},
  {match: {categoryNode: '1115193_1071966_1025741'}, mapTo: '623'},
  {match: {categoryNode: '1115193_1073264_1149389'}, mapTo: '624'},
  {match: {categoryNode: '1115193_1071966_1072133'}, mapTo: '7330'}];

const buildProduct = source => {
  const categoryMapper = _.find(categoryMap, mapper => _.matches(mapper.match)(source));
  const category = categoryMapper ? categories[categoryMapper.mapTo] : {};

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
