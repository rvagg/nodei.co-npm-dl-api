'use strict'

var moment     = require('moment')
  , db         = require('./db')


function pkgRank (pkg, callback) {
  db.packageRank(pkg, function afterGet (err, data) {
    if (err) {
      if (err.notFound)
        return callback(null, '')
      return callback(err)
    }

    if (moment(data.day) < moment().add(-3, 'days'))
      console.error('warning, ranking data for [' + pkg + '] is stale, from ' + data.day)

    callback(null, data.rank)
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

/*
function topDownloads (count, callback) {
  var date   = moment()
                .utcOffset(0)
                .subtract(1, 'days')
                .format('YYYY-MM-DD')
    , dsumDb = db.dateSumDb(date)

  function onErrorOrEnd (err, data) {
    if (!callback)
      return
    if (err)
      callback(err)
    else if (data) {
      data = data.map(function m (_d, i) {
        var d = JSON.parse(_d)

        d.rank = i + 1
        return d
      })
      callback(null, data)
    }
    callback = null
  }

  dsumDb
    .createValueStream({ reverse: true, limit: count })
    .on('error', onErrorOrEnd)
    .pipe(listStream.obj(onErrorOrEnd))
    .on('error', onErrorOrEnd)
}


function totalDownloads (callback) {
  var date   = moment()
                .utcOffset(0)
                .subtract(1, 'days')
                .format('YYYY-MM-DD')
    , dsumDb = db.dateSumDb(date)
    , total  = 0
    , pkgs   = 0

  function onErrorOrEnd (err) {
    if (!callback)
      return

    if (err)
      callback(err)
    else
      callback(null, { total: total, 'packages': pkgs })
    callback = null
  }

  dsumDb
    .createValueStream()
    .on('error', onErrorOrEnd)
    .on('data', function onData (value) {
      pkgs++
      total += JSON.parse(value).count
    })
    .on('error', onErrorOrEnd)
    .on('end', onErrorOrEnd)
}
*/


module.exports.pkgRank         = pkgRank
module.exports.pkgDownloadDays = pkgDownloadDays
module.exports.pkgDownloadSum  = pkgDownloadSum
// module.exports.topDownloads    = topDownloads
// module.exports.totalDownloads  = totalDownloads
