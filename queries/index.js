const Promise = require('bluebird');

const { getConnection } = require('../connection');
const { getUsersSessions, getCoursesSessionSummary } = require('./sessions');

module.exports = {
  groupDistribution(args) {
    const { courseId } = args;

    return new Promise((resolve, reject) => {
      const pipeline = [
        courseId ? { courseId } : null,
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
  },
  actionCountsByType(args) {
    const { courseId } = args;

    return new Promise((resolve, reject) => {
      const pipeline = [
        courseId ? { source: courseId } : null,
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
  },
  actionCountsByVariable(args) {
    const { courseId, variable } = args;

    return new Promise((resolve, reject) => {
      const pipeline = [
        courseId ? { source: courseId } : null,
        {
          $group: {
            _id: `$${variable}`,
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
  },
  sessionsByUser(args) {
    const { courseId, userId } = args;

    return getUsersSessions({ courseId, userId });
  },
  sessionSummaryByCourse(args) {
    const { courseId } = args;

    return getCoursesSessionSummary(courseId);
  },
}
