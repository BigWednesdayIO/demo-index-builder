'use strict';

const _ = require('lodash');
const request = require('request-promise');

const indexName = 'crateful-suggestions';

const stripPatterns = [
  '\\.|,|\\*', // punctuation
  '\\(.*\\)', // anything in brackets
  '1.*per outlet', // 1 kit/deal per outlet, 1 per outlet
];

const stripRegex = new RegExp(stripPatterns.join('|'), 'gm');

const cleanProductName = function(name) {
  return name
    .replace(stripRegex, '')
    .replace(/\s$/, '');
};

class SuggestionsIndex {
  constructor(apiBaseUrl, apiToken) {
    this._apiBaseUrl = `${apiBaseUrl}/indexes/${indexName}`;
    this._apiHeaders = {
      'content-type': 'application/json',
      authorization: `bearer ${apiToken}`
    }
  }

  delete() {
    return request.del({
      url:  this._apiBaseUrl,
      headers: this._apiHeaders
    })
    .then(() => {}, err => {
      if (err.statusCode === 404 && err.message.indexOf(indexName) >= 0) {
        return;
      }

      throw err;
    });
  }

  create() {
    return request.put({
      uri: `${this._apiBaseUrl}/settings`,
      headers: this._apiHeaders,
      body: {
        searchable_fields: ['query'],
        facets: []
      },
      json: true
    });
  }

  indexProductBatch(products) {
    const requests = products.map(product => {
      return {action: 'create', body: {query: cleanProductName(product.name)}};
    });

    return request.post({
      uri: `${this._apiBaseUrl}/batch`,
      headers: this._apiHeaders,
      body: {requests},
      json: true
    });
  }
}

module.exports = SuggestionsIndex;
