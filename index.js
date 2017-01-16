require('dotenv').config();

const Promise = require('bluebird');
const argv = require('minimist')(process.argv.slice(2));
const { isFunction } = require('lodash');

const { connect, getConnection } = require('./connection');
const queries = require('./queries');

function executeQuery(name) {
  if (isFunction(queries[name])) {
    return queries[name](argv);
  } else {
    return Promise.reject(`Query ${name} not found.`);
  }
}

console.log('Connecting to database...');

connect()
  .then(() => {
    console.log('Connected to database.')

    const queryName = argv['query'];

    if(!queryName) {
      return Promise.reject('No query provided.');
    } else {
      console.log(`Executing query ${queryName}...`);

      return executeQuery(queryName);
    }
  })
  .then(console.log)
  .catch(console.log);
