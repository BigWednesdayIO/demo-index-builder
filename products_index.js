'use strict';

const _ = require('lodash');
const request = require('request-promise');

const indexName = 'crateful-products';
const fieldsToIndex = ['name', 'brand', 'description', 'long_description'];

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

  throw new Error(`Un-mapped category/subcategory ${category}/${subcategory}`);
};

const buildCategoryHierachy = code => {
  if (!code) {
    return [];
  }

  const levels = code.split('.');
  return levels.map((level, index) => levels.slice(0, index + 1).join('.'));
};

const buildProduct = source => {
  const product = _.pick(source, fieldsToIndex);
  const category = getCategory(source.category, source.subcategory);
  product.category_code = category.code;
  product.category_hierachy = buildCategoryHierachy(category.code);
  product.category_desc = category.desc;
  return product;
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

  indexProductBatch(products) {
    const requests = [];

    products.forEach(p => {
      const product = buildProduct(p._source);

      for(let supplier of [{name: 'Pub Taverns', idPrefix: 'p'}, {name: 'Beer & Wine Co', idPrefix: 'b'}]) {
        const supplierProduct = _.clone(product);
        supplierProduct.supplier = supplier.name;

        requests.push({action: 'upsert', body: supplierProduct, objectID: `${supplier.idPrefix}${p._id}`});
      }
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
