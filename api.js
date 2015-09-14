'use strict'

const moment     = require('moment')
    , listStream = require('list-stream')
    , once       = require('once')
    , db         = require('./db')


function pkgRank (pkg, callback) {
  db.packageDb.get(pkg, (err, data) => {
    if (err) {
      if (err.notFound)
        return callback(null, '')
      return callback(err)
    }

    callback(null, JSON.parse(data))
  })
}


function pkgDownloadDays (pkg, days, callback) {
  let pkgCountDb   = db.packageCountDb(pkg)
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

    data = data.map((d) => ({ day: d.key, count: parseInt(d.value, 10) }))

    callback(null, data)
  }

  pkgCountDb
    .createReadStream({ gte: start, lt: end })
    .on('error', onErrorOrEnd)
    .pipe(listStream.obj(onErrorOrEnd))
    .on('error', onErrorOrEnd)
}


function pkgDownloadSum (pkg, days, callback) {
  pkgDownloadDays(pkg, days, (err, data) => {
    if (err)
      return callback(err)

    let sum = data.reduce((p, c) => p + c.count, 0)

    callback(null, sum)
  })
}


function topDownloads (count, callback) {
  let date   = moment()
                .utcOffset(0)
                .subtract(1, 'days')
                .format('YYYY-MM-DD')
    , dsumDb = db.dateSumDb(date)

  function onErrorOrEnd (err, data) {
    if (!callback)
      return
    if (err) {
      callback(err)
    } else if (data) {
      data = data.map((_d, i) => {
        let d = JSON.parse(_d)
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


module.exports.pkgRank         = pkgRank
module.exports.pkgDownloadDays = pkgDownloadDays
module.exports.pkgDownloadSum  = pkgDownloadSum
module.exports.topDownloads    = topDownloads
