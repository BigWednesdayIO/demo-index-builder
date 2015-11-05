'use strict';

const request = require('request-promise');
const _ = require('lodash');

const wallmartCategories = ['1229749_1086977_1086987', '1229749_1086977_1086990'];
const apiKey = 'zvu7f5zhqckaecazm3765jjw';
const walmartBaseUrl = 'http://api.walmartlabs.com';

const stripSpecialCharacters = sourceValue => (sourceValue || '').replace(/[\uFFF0-\uFFFF]*/g, '');

const normaliseString = sourceValue => {
  const normalised = (sourceValue || '')
                      .replace(/[\uFFF0-\uFFFF]*/g, '') // Special characters
                      .replace(/\s+/g, ' ');            // Whitespace
  return normalised || null;
};

module.exports = function(productsIndex, suggestionsIndex) {
  const categories = require('../categories.json');

  const categoryMap = [
    {match: {categoryNode: '1229749_1086977_1086987'}, mapTo: '4973'},
    {match: {categoryNode: '1229749_1086977_1086990'}, mapTo: '2530'},
    {match: {categoryNode: '1115193_1073264_1149383'}, mapTo: '2530'},
    {match: {categoryNode: '1115193_1073264_1149384'}, mapTo: '629'},
    {match: {categoryNode: '1115193_1071966_1025741'}, mapTo: '623'},
    {match: {categoryNode: '1115193_1073264_1149389'}, mapTo: '624'},
    {match: {categoryNode: '1115193_1071966_1072133'}, mapTo: '7330'}
  ];

  const buildProduct = source => {
    const categoryMapper = _.find(categoryMap, mapper => _.matches(mapper.match)(source));
    const category = categoryMapper ? categories[categoryMapper.mapTo] : {};

    return {
      name: normaliseString(source.name),
      brand: normaliseString(source.brandName),
      description: normaliseString(source.shortDescription),
      long_description: normaliseString(source.longDescription),
      supplier: 'Wallmart',
      category_code: category.hierachy,
      category_desc: category.name,
      price: source.salePrice,
      was_price: null
    }
  };

  const addBatchToIndex = response => {
    if (response && response.items) {
      const requests = response.items
                        .map(source => ({id: source.itemId, body: buildProduct(source)}))
                        .filter(source => !((source.body.brandName || '').startsWith('test_')) && source.body.category_code)
                        .map(source => ({
                          action: 'upsert',
                          body: source.body,
                          objectID: source.id.toString()
                        }));

      return Promise.all([
        productsIndex.indexProductBatch(requests),
        suggestionsIndex.indexProductBatch(_.map(requests, 'body'))
      ])
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

  const categoryIndexingJobs = wallmartCategories.map(categoryId => {
    return indexItems(`/v1/paginated/items?category=${categoryId}&apiKey=${apiKey}`)
              .then(() => {
                console.log(`Indexing of Wallmart category ${categoryId} complete`);
              });
  });

  return Promise.all(categoryIndexingJobs);
};
