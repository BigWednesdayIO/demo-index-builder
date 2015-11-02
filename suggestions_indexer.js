'use strict';

const os = require('os');
const fs = require('fs');
const _ = require('lodash');
const elasticsearch = require('elasticsearch');
const request = require('request-promise');

const elasticClient = new elasticsearch.Client({
  host: 'http://localhost:9200'
});

const fileName = 'suggestions.txt';

const settings = {
  searchable_fields: ['query'],
  facet_fields: []
};

const stripPatterns = [
  '\\.|,|\\*', // punctuation
  '\\(.*\\)', // anything in brackets
  '1.*per outlet', // 1 kit/deal per outlet, 1 per outlet
];

const removePatterns = [
  'DELIVERY',
  'FREE'
];

const stripRegex = new RegExp(stripPatterns.join('|'), 'gm');
const removeRegex = new RegExp(removePatterns.join('|'), 'gm');

const cleanProductName = function(name) {
  return name
    .replace(stripRegex, '')
    .replace(/\s$/, '');
};

let scrolling = false;
let processed = 0;
let total = 0;

const indexBatch = hits => {
  const requests = _(hits)
    .pluck('_source')
    .map(source => cleanProductName(source['name']))
    .value()
    .filter(name => !removeRegex.test(name) && name !== 'POINT')
    .map(name => {
      return {action: 'create', body: {query: name}};
    });

  const options = {
    method: 'POST',
    uri: 'https://api.bigwednesday.io/1/search/indexes/crateful-suggestions/batch',
    headers: {'content-type': 'application/json', authorization: 'bearer 8N*b3i[EX[s*zQ%'},
    body: {requests},
    json: true
  };

  return request.post(options);
};

const createSuggestionIndexSettingsOptions = {
  method: 'PUT',
  uri: 'https://api.bigwednesday.io/1/search/indexes/crateful-suggestions/settings',
  headers: {'content-type': 'application/json', authorization: 'bearer 8N*b3i[EX[s*zQ%'},
  body: {
    searchable_fields: ['query'],
    facet_fields: []
  },
  json: true
};

request.put(createSuggestionIndexSettingsOptions).then(() => {
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
          console.log(`Indexed suggestions for ${processed}/${total} products.`)
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
      console.log(`${total} products to process...`);
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
