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

  if (response.hits.hits.length > 0) {
    const productNames = _(response.hits.hits)
      .pluck('_source')
      .map(source => cleanProductName(source['name']))
      .value()
      .filter(name => !removeRegex.test(name) && name !== 'POINT');

    fs.appendFileSync(fileName, `${productNames.join(os.EOL)}${os.EOL}`);

    processed += response.hits.hits.length;
    console.log(`Processed ${processed}/${total} products.`)
  }
  else if (scrolling) {
    //
    console.log('Complete');
    return process.exit(0);
  }
  else {
    total = response.hits.total;
    console.log(`${total} products to process...`);
  }

  scrolling = true;

  return elasticClient.scroll({
    scrollId: response._scroll_id,
    scroll: '30s'
  }, getMoreUntilDone);
});
