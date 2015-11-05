'use strict';

const _ = require('lodash');
const request = require('request-promise');

const indexName = 'crateful-products';

const buildCategoryHierachy = code => {
  if (!code) {
    return [];
  }

  const levels = code.split('.');
  return levels.map((level, index) => levels.slice(0, index + 1).join('.'));
};

class ProductsIndex {
  constructor(apiBaseUrl, apiToken) {
    this._apiBaseUrl = `${apiBaseUrl}/indexes/${indexName}`;
    this._apiHeaders = {
      'content-type': 'application/json',
      authorization: `bearer ${apiToken}`
    };
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
        searchable_fields: ['name', 'category_desc', 'brand'],
        facet_fields: ['supplier', 'category_code', 'brand']
      },
      json: true
    });
  }

  indexProductBatch(indexingRequests) {
    const requests = indexingRequests.map(r => {
      r.body.category_hierarchy = buildCategoryHierachy(r.body.category_code);
      return r;
    });

    return request.post({
      uri: `${this._apiBaseUrl}/batch`,
      headers: this._apiHeaders,
      body: {requests},
      json: true
    });
  }
}

module.exports = ProductsIndex;
