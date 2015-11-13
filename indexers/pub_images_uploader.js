'use strict';

const _ = require('lodash');
const bluebird = require('bluebird');
const cloudinary = require('cloudinary');
const fs = require('fs');

const stat = bluebird.promisify(fs.stat);

process.env.CLOUDINARY_URL='cloudinary://947268211952775:o85OQyfoU4mLu9ux2nNjUTHT_wU@dc3gcqic2';

const uploadImage = (path, id) => {
  return new Promise(resolve => {
    cloudinary.uploader.upload(path, resolve, {public_id: id});
  });
};

const toImageId = productId => {
  return `${productId}_A_p`;
};

const imagePath = imageId => {
  return `./pub_images/${imageId[0]}/${imageId[1]}/${imageId[2]}/${imageId}.jpg`;
};

module.exports = function uploadImages (productIds) {
  const imageIds = productIds.map(toImageId);

  return new Promise(resolve => {
    cloudinary.api.resources_by_ids(imageIds, result => {
      const imagesToUpload = _.reject(imageIds, id => _.find(result.resources, {public_id: id}));

      const uploadJobs = imagesToUpload.map(imageId => {
        return {id: imageId, path: imagePath(imageId)};
      });

      resolve(uploadJobs);
    });
  })
  .then(jobs => {
    if (jobs.length) {
      console.log(`Uploading images ${_.pluck(jobs, 'id')}`);
    }

    return Promise.all(jobs.map(job => uploadImage(job.path, job.id)));
  })
  .then(() => {
    console.log('Images uploaded');
  });
};

module.exports.productHasImage = function(productId) {
  return stat(imagePath(toImageId(productId)));
};
