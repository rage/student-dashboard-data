const Promise = require('bluebird');
const path = require('path');

const { getConnection } = require('../connection');
const { getUsersSessions, getCoursesSessionSummary } = require('./sessions');
const { setParticipatingDocuments } = require('./participations');
const { setOrientationToDocuments } = require('./goal-orientations');

function participatingMatcher(query) {
  return Object.assign({}, query, { participating: true });
}

function groupDistribution(args) {
  const { courseId } = args;

  return new Promise((resolve, reject) => {
    const match = courseId ? { courseId: courseId.toString() } : {};

    const pipeline = [
      { $match: participatingMatcher(match) },
      {
        $group: {
          _id: '$group',
          count: { $sum: 1 }
        }
      }
    ].filter(p => !!p);

    getConnection()
      .collection('participants')
      .aggregate(pipeline)
      .toArray((err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
  });
}

function actionCountsByType(args) {
  const { courseId } = args;

  return new Promise((resolve, reject) => {
    const match = courseId ? { courseId: courseId.toString() } : {};

    const pipeline = [
      { $match: participatingMatcher(match) },
      {
        $group: {
          _id: { visualizationType: '$meta.visualizationType', actionName: '$name', userId: '$userId' },
          count: { $sum: 1 }
        }
      }
    ].filter(p => !!p);

    getConnection()
      .collection('actions')
      .aggregate(pipeline)
      .toArray((err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
  });
}

function actionCountsByTime(args) {
  const { courseId, variable } = args;

  return new Promise((resolve, reject) => {
    const match = courseId ? { source: courseId.toString() } : {};

    const pipeline = [
      { $match: participatingMatcher(match) },
      {
        $group: {
          _id: { weekday: '$weekday', hour: '$hour' },
          count: { $sum: 1 }
        }
      }
    ].filter(p => !!p);

    getConnection()
      .collection('actions')
      .aggregate(pipeline)
      .toArray((err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
  });
}

function sessionsByUser(args) {
  const { courseId, userId } = args;

  return getUsersSessions({ courseId, userId });
}

function sessionSummaryByCourse(args) {
  const { courseId } = args;

  return getCoursesSessionSummary(courseId);
}

function preparations(args) {
  const { orientationSurveyResults, courseId } = args;

  const surveyResultsPath = path.resolve(__dirname, '..', 'data', orientationSurveyResults);
  const params = { orientationSurveyResults: surveyResultsPath, courseId: courseId.toString() };

  return Promise.resolve([
    setParticipatingDocuments(params),
    setOrientationToDocuments(params),
  ]).then(() => null);
}

module.exports = {
  groupDistribution,
  actionCountsByType,
  actionCountsByTime,
  sessionsByUser,
  sessionSummaryByCourse,
  preparations,
};
