const hl = require('highland');
const _ = require('lodash');

const { getConnection } = require('../connection');

function getUsersSessions({ userId, courseId }) {
  const query = { userId: userId.toString(), source: courseId.toString() };

  const options = {
    sort: [['userId', 'asc'], ['createdAtAdjustedToTz', 'asc']]
  };

  const stream = getConnection().collection('actions').find(query, options);

  let sessionId = 0;

  const sessions = {};

  stream.on('data', action => {
    if (action.name === 'OPEN_PLUGIN') {
      const previousSessionId = sessionId.toString();

      if (sessions[previousSessionId] && !sessions[previousSessionId].actions.includes('CLOSE_PLUGIN')) {
        delete sessions[previousSessionId];
      }

      sessionId = sessionId + 1;
    }

    sessions[sessionId.toString()] = sessions[sessionId.toString()] || { actions: [], started: action.createdAt, ended: null, duration: null };
    sessions[sessionId.toString()].actions.push(action.name);

    if (action.name === 'CLOSE_PLUGIN') {
      sessions[sessionId.toString()].ended = action.createdAt;

      const { ended, started } = sessions[sessionId.toString()];

      sessions[sessionId.toString()].duration = +new Date(ended) - +new Date(started);
    }
  });

  return new Promise((resolve, reject) => {
    stream.on('end', () => {
      const closedSessions = _.omitBy(sessions, value => !value.duration);

      return _.keys(closedSessions).length === 0
        ? resolve(null)
        : resolve(closedSessions);
    });

    stream.on('error', reject);
  });
}

function getCoursesSessionSummary(courseId) {
  const stream = getConnection().collection('participants').find({ courseId: courseId.toString() });

  const sessions = {
    users: {},
    total: {}
  };

  return new Promise((resolve, reject) => {
    hl(stream)
      .map(participant => {
        const promise = getUsersSessions({ userId: participant.userId, courseId })
          .then(data => {
            return data
              ? { data, meta: { userId: participant.userId } }
              : null;
          });

        return hl(promise);
      })
      .parallel(20)
      .each(sessionData => {
        if (sessionData) {
          const userSessions = _.values(sessionData.data);

          _.set(sessions, ['users', sessionData.meta.userId], {
            actions: _.meanBy(userSessions, data => data.actions.length),
            duration: _.meanBy(userSessions, data => data.duration) / 1000
          });
        }
      })
      .done(() => {
        const allUsersSessions = _.values(sessions.users);

        sessions.total = {
          actions: _.meanBy(allUsersSessions, data => data.actions),
          duration: _.meanBy(allUsersSessions, data => data.duration)
        };

        resolve(sessions);
      });
  });
}

module.exports = {
  getUsersSessions,
  getCoursesSessionSummary
};
