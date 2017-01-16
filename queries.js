const Promise = require('bluebird');

const { getConnection } = require('./connection');

module.exports = {
  groupDistribution(args) {
    return new Promise((resolve, reject) => {
      getConnection()
        .collection('participants')
        .aggregate([
          { $group: { _id: '$group', count: { $sum: 1 } } }
        ])
        .toArray((err, result) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        });
    });
  },
}
