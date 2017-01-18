const hl = require('highland');
const jsonStream = require('JSONStream');
const Promise = require('bluebird');
const { createReadStream } = require('fs');
const mirror = require('keymirror');
const _ = require('lodash');

const { getConnection } = require('../connection');
const { updateAsync } = require('../utils');

const orientations = mirror({
  MASTERY_APPROACH: null,
  MASTERY_AVOIDANCE: null,
  PERFORMANCE_APPROACH: null,
  PERFORMANCE_AVOIDANCE: null,
});

function getParticipantToAnswer(filePath) {
  const stream = createReadStream(filePath);

  return new Promise((resolve, reject) => {
    hl(stream)
      .through(jsonStream.parse('*'))
      .reduce({}, (mapper, answer) => {
        const { answererId, data } = answer;

        mapper[answererId] = data;

        return mapper;
      })
      .toCallback((err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
  });
}

function getOrientation(answer) {
  const itemToOrientation = {
    'ixuap8t8w': orientations.PERFORMANCE_APPROACH,
    'ixub07yzx': orientations.PERFORMANCE_APPROACH,
    'ixub1mr2p': orientations.PERFORMANCE_APPROACH,
    'ixub1nna1i': orientations.MASTERY_AVOIDANCE,
    'ixub1obi2': orientations.MASTERY_AVOIDANCE,
    'ixub1qih1h': orientations.MASTERY_AVOIDANCE,
    'ixub9bxtg': orientations.MASTERY_APPROACH,
    'ixub9vqn1x': orientations.MASTERY_APPROACH,
    'ixubaqs21k': orientations.MASTERY_APPROACH,
    'ixubbbnr2p': orientations.PERFORMANCE_AVOIDANCE,
    'ixubdzd21g': orientations.PERFORMANCE_AVOIDANCE,
    'ixubfrphl': orientations.PERFORMANCE_AVOIDANCE,
  };

  const orientation = _.chain(answer)
    .toPairs()
    .groupBy(pair => itemToOrientation[pair[0]] || '_')
    .mapValues(value => _.sum(value.map(item => item[1])))
    .toPairs()
    .maxBy(pair => pair[1])
    .value()[0];

  return orientation === '_'
    ? null
    : orientation;
}

function setOrientationToUsersDocuments({ userId, courseId, orientation }) {
  const connection = getConnection();
  const update = { $set: { orientation } };
  const options = { multi: true };

  return Promise.all([
    updateAsync({ connection, collection: 'participants', query: { userId, courseId }, update, options }),
    updateAsync({ connection, collection: 'actions', query: { userId, source: courseId }, update, options }),
  ]);
}

function setOrientationToDocuments({ courseId, orientationSurveyResults }) {
  return getParticipantToAnswer(orientationSurveyResults)
    .then(participantToAnswer => {
      const stream = getConnection().collection('participants').find({ courseId });

      return new Promise((resolve, reject) => {
        hl(stream)
          .map(participant => {
            const { userId } = participant;
            const answer = participantToAnswer[userId];
            const orientation = answer
              ? getOrientation(answer)
              : null;

            return hl(setOrientationToUsersDocuments({ userId, courseId, orientation }));
          })
          .parallel(20)
          .stopOnError(reject)
          .done(resolve);
      });
    });
}

module.exports = {
  orientations,
  setOrientationToDocuments,
};
