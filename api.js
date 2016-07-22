'use strict'

var moment     = require('moment')
  , db         = require('./db')


function transformRankData (data) {
  return {
      rank           : data.rank
    , date           : data.day
    , total          : data.packageCount || (db.allPackages && db.allPackages.length)
    , downloads      : data.count
    , totalDownloads : db.periodAllTotal
  }
}


function pkgRank (pkg, callback) {
  db.packageRank(pkg, function afterGet (err, data) {
    if (err) {
      if (err.notFound)
        return callback(null, '')
      return callback(err)
    }

    if (moment(data.day) < moment().add(-3, 'days'))
      console.error('warning, ranking data for [' + pkg + '] is stale, from ' + data.day)

    callback(null, transformRankData(data))
  })
}


function _pkgDownloadRangeFunction (fn, pkg, days, callback) {
  var start        = moment().utc()
                      .subtract(days + 1, 'days')
                      .format('YYYY-MM-DD')
    , end          = moment().utc(0)
                      .subtract(1, 'days')
                      .format('YYYY-MM-DD')

  db[fn](pkg, start, end, callback)
}

function pkgDownloadDays (pkg, days, callback) {
  return _pkgDownloadRangeFunction('packageCounts', pkg, days, callback)
}


function pkgDownloadSum (pkg, days, callback) {
  return _pkgDownloadRangeFunction('packageCount', pkg, days, callback)
}

function topDownloads (count, callback) {
  return db.topPackages(count, function afterTop (err, data) {
    if (err)
      return callback(err)

    callback(null, data.map(transformRankData))
  })
}


function totalDownloads (callback) {
  setImmediate(function i () {
    callback(null, {
        total    : db.periodAllTotal
      , packages : db.allPackages && db.allPackages.length
    })
  })
}


module.exports.pkgRank         = pkgRank
module.exports.pkgDownloadDays = pkgDownloadDays
module.exports.pkgDownloadSum  = pkgDownloadSum
module.exports.topDownloads    = topDownloads
module.exports.totalDownloads  = totalDownloads
