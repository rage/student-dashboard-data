const Promise = require('bluebird');

function updateAsync({ connection, collection, query, update, options }) {
  return new Promise((resolve, reject) => {
    connection
      .collection(collection)
      .update(query, update, options, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
  });
}

module.exports = {
  updateAsync,
};
