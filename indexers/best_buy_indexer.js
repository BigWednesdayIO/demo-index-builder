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

module.exports = function(productsIndex, suggestionsIndex) {
  return new Promise((resolve, reject) => {
    MongoClient.connect('mongodb://localhost:27017/bestbuy', function(err, db) {
      if (err) {
        throw new Error('Couldn\'t connect to mongodb bestbuy database');
      }

      let batch = [];
      let totalIndexed = 0;

      const buildProduct = function(bestBuyProduct, enc, next) {
        const product = {
          body: {
            name: bestBuyProduct.name,
            brand: bestBuyProduct.manufacturer,
            description: bestBuyProduct.shortDescription,
            long_description: bestBuyProduct.longDescription,
<<<<<<< HEAD
            price: bestBuyProduct.salePrice ,
            was_price: bestBuyProduct.onSale ? bestBuyProduct.regularPrice : null,
=======
>>>>>>> index product names as suggestions for bestbuy products
            supplier: 'Best Buy'
          },
          objectID: bestBuyProduct.sku.toString()
        };

        const productCategories = bestBuyProduct.categoryPath;
        productCategories.reverse();

        for(const category of productCategories) {
          const mappedCategory = _.find(categoryMap, {bestbuy: category.id});

          if (mappedCategory) {
            product.body.category_code = mappedCategory.google_code;
            product.body.category_desc = mappedCategory.google_desc;
            break;
          }
        }

        this.push(product);
        next();
      };

      const batchProducts = function(product, enc, next) {
        batch.push(product);

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
        const indexProductRequests = _.map(batch, product => {
          return {action: 'upsert', body: product.body, objectID: product.objectID};
        });

        Promise.all([
          productsIndex.indexProductBatch(indexProductRequests),
          suggestionsIndex.indexProductBatch(_.map(batch, 'body'))
        ])
        .then(() => {
          totalIndexed += batch.length;
          console.log(`Indexed ${totalIndexed} products from Best Buy`);
          next();
        }, next);
      };

      db.collection('products').find({'categoryPath.id': {$in: _.pluck(categoryMap, 'bestbuy')}})
        .pipe(through2.obj(buildProduct))
        .pipe(through2.obj(batchProducts, endBatching))
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
