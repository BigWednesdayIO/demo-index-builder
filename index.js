'use strict';

const _ = require('lodash');
const elasticsearch = require('elasticsearch');
const request = require('request-promise');

const elasticClient = new elasticsearch.Client({
  host: 'http://localhost:9200'
});

const fieldsToIndex = ['name', 'brand', 'category', 'description', 'long_description'];

const settings = {
  searchable_fields: ['name', 'category', 'brand'],
  facet_fields: ['category', 'brand']
};

const indexBatch = hits => {
  const requests = [];

  hits.forEach(hit => {
    const product = _.pick(hit._source, fieldsToIndex);

    for(let supplier of [{name: 'Pub Taverns', idPrefix: 'p'}, {name: 'Beer & Wine Co', idPrefix: 'b'}]) {
      const supplierProduct = _.clone(product);
      supplierProduct.supplier = supplier.name;

      requests.push({action: 'upsert', body: supplierProduct, objectID: `${supplier.idPrefix}${hit._id}`});
    }
  });

  var options = {
    method: 'POST',
    uri: 'https://api.bigwednesday.io/1/search/indexes/crateful-products/batch',
    headers: {'content-type': 'application/json', authorization: 'bearer 8N*b3i[EX[s*zQ%'},
    body: {requests},
    json: true
  };

  return request.post(options);
};

const additionalSuppliers = ['']

let scrolling = false;
let processed = 0;
let total = 0;

var createSettingsOptions = {
  method: 'PUT',
  uri: 'https://api.bigwednesday.io/1/search/indexes/crateful-products/settings',
  headers: {'content-type': 'application/json', authorization: 'bearer 8N*b3i[EX[s*zQ%'},
  body: settings,
  json: true
};

request.put(createSettingsOptions).then(() => {
  elasticClient.search({
    index: 'demo',
    scroll: '30s',
    search_type: 'scan',
    type: ['product'],
    q: '*'
  }, function getMoreUntilDone(err, response) {
    if (err) {
      console.error('Retrieval error. Aborting.')
      console.error(err);
      return process.exit(1);
    }

    let result = Promise.resolve({});

    if (response.hits.hits.length > 0) {
      result = indexBatch(response.hits.hits)
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
});
