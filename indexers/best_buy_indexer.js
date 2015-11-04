'use strict';

const _ = require('lodash');
const through2 = require('through2');
const MongoClient = require('mongodb').MongoClient;

const categoryMap = [
  {bestbuy: 'abcat0902003', google_code: '536.638.730.4161', google_desc: 'Ice Makers'},
  {bestbuy: 'abcat0901007', google_code: '536.638.730.4539', google_desc: 'Wine Fridges'},
  {bestbuy: 'abcat0101000', google_code: '222.386.404', google_desc: 'Televisions'},
  {bestbuy: 'abcat0102000', google_code: '222.386.387.388', google_desc: 'DVD & Blu-ray Players'},
  {bestbuy: 'pcmcat161100050040', google_code: '222.386.387.5276', google_desc: 'Media Streaming Devices'},
  {bestbuy: 'abcat0202002', google_code: '222.223.242.251', google_desc: 'Stereo Systems'},
  {bestbuy: 'abcat0202001', google_code: '222.223.242.226', google_desc: 'CD Players & Recorders'},
  {bestbuy: 'pcmcat211400050001', google_code: '222.342.1350.5497', google_desc: 'Wireless Routers'},
  {bestbuy: 'abcat0503012', google_code: '222.342.2455', google_desc: 'Hubs & Switches'},
  {bestbuy: 'abcat0503013', google_code: '222.342.343', google_desc: 'Modems'},
  {bestbuy: 'pcmcat254000050005', google_code: '141.142.362', google_desc: 'Surveillance Cameras'},
  {bestbuy: 'pcmcat340500050007', google_code: '141.142.362', google_desc: 'Surveillance Cameras'},
  {bestbuy: 'pcmcat308100050021',  google_code: '141.2096.143.5937', google_desc: 'Surveillance Camera Accessories'}
];

const batchSize = 25;

module.exports = function(productsIndex) {
  return new Promise((resolve, reject) => {
    MongoClient.connect('mongodb://localhost:32771/bestbuy', function(err, db) {
      if (err) {
        throw new Error('Couldn\'t connect to mongodb bestbuy database');
      }

      let batch = [];
      let totalIndexed = 0;

      const toBatchUpsertRequest = function(product, enc, next) {
        const request = {
          action: 'upsert',
          body: {
            name: product.name,
            brand: product.manufacturer,
            description: product.shortDescription,
            long_description: product.longDescription,
            supplier: 'Best Buy'
          },
          objectID: product.sku.toString()
        };

        const productCategories = product.categoryPath;
        productCategories.reverse();

        for(const category of productCategories) {
          const mappedCategory = _.find(categoryMap, {bestbuy: category.id});

          if (mappedCategory) {
            request.body.category_code = mappedCategory.google_code;
            request.body.category_desc = mappedCategory.google_desc;
            break;
          }
        }

        this.push(request);
        next();
      };

      const batchRequests = function(request, enc, next) {
        batch.push(request);

        if (batch.length === batchSize) {
          this.push(batch);
          batch = [];
        }

        next();
      };

      const endBatching = function(next) {
        if (batch.length) {
          this.push(batch);
        }

        next();
      };

      const indexBatch = function(batch, enc, next) {
        productsIndex.indexProductBatch(batch)
          .then(() => {
            totalIndexed += batch.length;
            console.log(`Indexed ${totalIndexed} products from Best Buy`);
            next();
          }, next);
      };

      db.collection('products').find({'categoryPath.id': {$in: _.pluck(categoryMap, 'bestbuy')}})
        .pipe(through2.obj(toBatchUpsertRequest))
        .pipe(through2.obj(batchRequests, endBatching))
        .pipe(through2.obj(indexBatch, () => {
          db.close();
        }))
        .on('error', err => {
          console.error('Error indexing Best Buy data');
          reject(err);
          db.close();
        });
    });
  });
};
