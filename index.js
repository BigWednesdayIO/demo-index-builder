'use strict';

const _ = require('lodash');
const elasticsearch = require('elasticsearch');
const request = require('request-promise');

const ProductsIndex = require('./products_index');
const SuggestionsIndex = require('./suggestions_index');

const apiBaseUrl = 'https://api.bigwednesday.io/1/search';

const productsIndex = new ProductsIndex(apiBaseUrl, '8N*b3i[EX[s*zQ%');
const suggestionsIndex = new SuggestionsIndex(apiBaseUrl, '8N*b3i[EX[s*zQ%');

const elasticClient = new elasticsearch.Client({
  host: 'http://localhost:9200'
});

let scrolling = false;
let processed = 0;
let total = 0;

Promise.all([
  suggestionsIndex.delete(),
  productsIndex.delete()
])
.then(() => Promise.all([
  suggestionsIndex.create(),
  productsIndex.create()
]))
.then(() => {
  elasticClient.search({
      index: 'demo',
      scroll: '30s',
      search_type: 'scan',
      type: ['product'],
      body: {
        "query": {
          "bool": {
            "must_not": [
              {
                "terms": {
                  "brand.raw": [
                    ".",
                    "Delivery Charge (Fri Same Day)",
                    "Delivery Charge (Off Day)"
                  ]
                }
              }
            ]
          }
        }
      }
    }, function getMoreUntilDone(err, response) {
      if (err) {
        console.error('Retrieval error. Aborting.')
        console.error(err);
        return process.exit(1);
      }

      let result = Promise.resolve({});

      if (response.hits.hits.length > 0) {
        const products = response.hits.hits;

        result = Promise.all([
            productsIndex.indexProductBatch(products),
            suggestionsIndex.indexProductBatch(products)
        ])
        .then(bulkResponse => {
          if (bulkResponse.errors) {
            console.error('Indexing error. Aborting.')
            console.log(JSON.stringify(bulkResponse));
            process.exit(1);
          }

          processed += response.hits.hits.length;
          console.log(`Indexed ${processed}/${total} products.`)
        })
        .catch(err => {
          console.error('Indexing error. Aborting.')
          console.log(err);
          process.exit(1);
        });
      }
      else if (scrolling) {
        console.log('Complete');
        return process.exit(0);
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
        console.error('Error. Aborting.');
        console.error(err);
        process.exit(1);
      });
    });
})
.catch(err => {
  console.error('Error. Aborting.');
  console.error(err);
  process.exit(1);
});
