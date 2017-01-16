const Promise = require('bluebird');

Promise.promisify(require('mongodb'));

const { MongoClient } = require('mongodb');

let connection = null;

function connect() {
  return new Promise((resolve, reject) => {
    MongoClient.connect(process.env.MONGO_URI, (err, db) => {
      if (err) {
        reject(err);
      } else {
        connection = db;

        resolve(db);
      }
    });
  });
}

function getConnection() {
  return connection;
}

module.exports = {
  connect,
  getConnection,
}
