'use strict';

const _ = require('lodash');
const elasticsearch = require('elasticsearch');
const request = require('request-promise');

const elasticClient = new elasticsearch.Client({
  host: 'http://localhost:9200'
});

const fieldsToIndex = ['name', 'brand', 'description', 'long_description'];

const settings = {
  searchable_fields: ['name', 'category_desc', 'brand'],
  facet_fields: ['supplier', 'category_code', 'brand']
};

const getCategory = (category, subcategory) => {
  if (_.include(['AleStout', 'Lager', 'FinestCask'], category)) {
    return {
      code: '412.413.499676.414',
      desc: 'Beer'
    };
  }

  if (category === 'Wine') {
    return {
      code: '412.413.499676.421',
      desc: 'Wine'
    };
  }

  if (category === 'Spirits') {
    if (subcategory === 'Liqueurs') {
      return {
        code: '412.413.499676.417.2933',
        desc: 'Liqueurs'
      };
    }
    if (subcategory === 'Vodka') {
      return {
        code: '412.413.499676.417.2107',
        desc: 'Vodka'
      };
    }
    if (subcategory === 'Brandy') {
      return {
        code: '412.413.499676.417.2364',
        desc: 'Brandy'
      };
    }
    if (subcategory === 'Sambuca') {
      return {
        code: '412.413.499676.417.2933',
        desc: 'Sambuca'
      };
    }
    if (_.include(['White Rum', 'Dark and Golden Rum', 'Dark & Golden Rum'])) {
      return {
        code: '412.413.499676.417.2605',
        desc: 'Rum'
      };
    }
    if (_.include(['Blended Whisky', 'Malt Whisky', 'Imported Whisky'])) {
      return {
        code: '412.413.499676.417.1926',
        desc: '1926'
      };
    }
    if (subcategory === 'Gin') {
      return {
        code: '412.413.499676.417.1671',
        desc: 'Gin'
      };
    }

    return {
      code: '417',
      desc: 'Liquor & Spirits'
    };
  }

  if (category === 'SoftDrinks') {
    if (subcategory === 'Energy Drinks') {
      return {
        code: '412.413.5723',
        desc: 'Sports & Energy Drinks'
      };
    }
    if (subcategory === 'Juices') {
      return {
        code: '412.413.2887',
        desc: 'Juice'
      };
    }
    if (subcategory === 'Mixers') {
      return {
        code: '412.413.499676.5725',
        desc: 'Cocktail Mixes'
      };
    }
    if (subcategory === 'Water') {
      return {
        code: '412.413.420',
        desc: 'Water'
      };
    }
    if (subcategory === 'Cordials') {
      return {
        code: '412.413.8036',
        desc: 'Fruit-Flavoured Drinks'
      };
    }
    if (subcategory === 'Canned Drinks') {
      return {
        code: '412.413.2628',
        desc: 'Fizzy Drinks'
      };
    }

    return {
      code: '412.413',
      desc: 'Beverages'
    };
  }

  if (category === 'Cider') {
    return {
      code: '412.413.499676.6761',
      desc: 'Cider'
    };
  }

  if (category === 'POS') {
    return {
      code: '111.5863',
      desc: 'Advertising & Marketing'
    };
  }

  if (category === 'ReadyToDrink') {
    return {
      code: '412.413.499676.5887',
      desc: 'Flavoured Alcoholic Beverages'
    };
  }

  if (category === 'Sundries') {
    if (subcategory === 'Co2') {
      return {
        code: '632.502975',
        desc: 'Fuel Containers & Tanks'
      };
    }
    if (subcategory === 'Cleaning Products') {
      return {
        code: '536.630.623.4973',
        desc: 'Household Cleaning Products'
      };
    }
  }

  if (category === 'Other') {
    return {
      code: '',
      subcategory: ''
    };
  }

  console.error(`Un-mapped category/subcategory ${category}/${subcategory}`);
  process.exit(1);
};

const buildProduct = source => {
  const product = _.pick(source, fieldsToIndex);
  const category = getCategory(source.category, source.subcategory);
  product.category_code = category.code;
  product.category_desc = category.desc;
  return product;
};

const indexBatch = hits => {
  const requests = [];

  hits.forEach(hit => {
    const product = buildProduct(hit._source);

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
