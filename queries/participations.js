const hl = require('highland');
const jsonStream = require('JSONStream');
const Promise = require('bluebird');
const { createReadStream } = require('fs');

const { getConnection } = require('../connection');
const { updateAsync } = require('../utils');

function getOrientationSurveyParticipants(filePath) {
  const stream = createReadStream(filePath);

  return new Promise((resolve, reject) => {
    hl(stream)
      .through(jsonStream.parse('*.answererId'))
      .stopOnError(reject)
      .toArray(resolve)
  });
}

function hasOpenedPlugin(userId) {
  return new Promise((resolve, reject) => {
    getConnection()
      .collection('actions')
      .find({ userId, name: 'OPEN_PLUGIN' }, { limit: 1 })
      .toArray((err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result.length > 0);
        }
      });
  });
}

function participantIsParticipating({ userId, orientationSurveyParticipants }) {
  return Promise.all([
    hasOpenedPlugin(userId),
    Promise.resolve(orientationSurveyParticipants.includes(userId))
  ]).then(results => results.every(result => result === true));
}

function setParticipatingStatusForCollection({ status, collection, query }) {
  return new Promise((resolve, reject) => {
    getConnection()
      .collection(collection)
      .update(query, { $set: { participating: status } }, { multi: true }, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
  });
}

function setParticipatingStatus({ userId, courseId, status }) {
  const connection = getConnection();
  const update = { $set: { participating: status } };
  const options = { multi: true };

  return Promise.all([
    updateAsync({ connection, collection: 'participants', query: { userId, courseId }, update, options }),
    updateAsync({ connection, collection: 'actions', query: { userId, source: courseId }, update, options }),
  ]);
}

function setParticipatingDocuments({ orientationSurveyResults, courseId }) {
  return getOrientationSurveyParticipants(orientationSurveyResults)
    .then(orientationSurveyParticipants => {
      const stream = getConnection().collection('participants').find({ courseId });

      return new Promise((resolve, reject) => {
        hl(stream)
          .map(participant => {
            const { group, userId } = participant;

            const getIsParticipating = group === 0
              ? () => Promise.resolve(true)
              : () => participantIsParticipating({ userId, orientationSurveyParticipants, courseId });

            const promise = getIsParticipating()
              .then(isParticipating => setParticipatingStatus({ userId, courseId, status: isParticipating }));

            return hl(promise);
          })
          .parallel(20)
          .stopOnError(reject)
          .done(resolve);
      });
    });
}

module.exports = {
  setParticipatingDocuments,
};
