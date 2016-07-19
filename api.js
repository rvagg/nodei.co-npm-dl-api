'use strict'

var moment     = require('moment')
  , listStream = require('list-stream')
  , once       = require('once')
  , db         = require('./db')


function pkgRank (pkg, callback) {
  db.packageDb.get(pkg, function afterGet (err, data) {
    if (err) {
      if (err.notFound)
        return callback(null, '')
      return callback(err)
    }

    callback(null, JSON.parse(data))
  })
}


function pkgDownloadDays (pkg, days, callback) {
  var pkgCountDb   = db.packageCountDb(pkg)
    , start        = moment()
                      .utcOffset(0)
                      .subtract(days + 1, 'days')
                      .format('YYYY-MM-DD')
    , end          = moment()
                      .utcOffset(0)
                      .subtract(1, 'days')
                      .format('YYYY-MM-DD')
    , onErrorOrEnd = once(_onErrorOrEnd)

  function _onErrorOrEnd (err, data) {
    if (err)
      return callback(err)

    data = data.map(function m (d) { return { day: d.key, count: parseInt(d.value, 10) } })

    callback(null, data)
  }

  pkgCountDb
    .createReadStream({ gte: start, lt: end })
    .on('error', onErrorOrEnd)
    .pipe(listStream.obj(onErrorOrEnd))
    .on('error', onErrorOrEnd)
}


function pkgDownloadSum (pkg, days, callback) {
  pkgDownloadDays(pkg, days, function afterDays (err, data) {
    var sum

    if (err)
      return callback(err)

    sum = data.reduce(function r (p, c) { return p + c.count }, 0)

    callback(null, sum)
  })
}


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


module.exports.pkgRank         = pkgRank
module.exports.pkgDownloadDays = pkgDownloadDays
module.exports.pkgDownloadSum  = pkgDownloadSum
module.exports.topDownloads    = topDownloads
module.exports.totalDownloads  = totalDownloads
