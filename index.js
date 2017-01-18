require('dotenv').config();

const path = require('path');
const { writeFile } = require('fs');
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

    if (!queryName) {
      return Promise.reject('No query provided.');
    } else {
      console.log(`Executing query ${queryName}...`);

      return executeQuery(queryName);
    }
  })
  .then(data => {
    if(argv['outfile']) {
      writeFile(path.join(__dirname, 'data', argv['outfile']), JSON.stringify(data), err => {
        if (err) {
          console.log(`Couldn't write output to file ${argv['outfile']}.`)
          console.log(err);
        } else {
          console.log(`Output has been written to file ${argv['outfile']}.`);
          process.exit(1);
        }
      });
    } else {
      console.log(data);
      process.exit(1);
    }
  })
  .catch(console.log);
