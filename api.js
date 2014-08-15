const moment     = require('moment')
    , listStream = require('list-stream')
    , db         = require('./db')


function pkgRank (pkg, callback) {
  db.packageDb.get(pkg, { valueEncoding: 'json' }, callback)
}


function pkgDownloads (pkg, days, callback) {
  var pkgCountDb = db.packageCountDb(pkg)
    , start      = moment().zone(0).subtract('days', days + 1).format('YYYY-MM-DD')
    , end        = moment().zone(0).subtract('days', 1).format('YYYY-MM-DD')

  function onErrorOrEnd (err, data) {
    if (!callback)
      return
    if (err) {
      callback(err)
    } else if (data) {
      callback(null, data.map(function (d) {
        return { day: d.key, count: parseInt(d.value, 10) }
      }))
    }
    callback = null
  }

  pkgCountDb
    .createReadStream({ gte: start, lt: end, valueEncoding: 'json' })
    .on('error', onErrorOrEnd)
    .pipe(listStream.obj(onErrorOrEnd))
    .on('error', onErrorOrEnd)
}


function topDownloads (count, callback) {
  var dsumDb = db.dateSumDb(moment().zone(0).subtract('days', 1).format('YYYY-MM-DD'))

  function onErrorOrEnd (err, data) {
    if (!callback)
      return
    if (err) {
      callback(err)
    } else if (data) {
      callback(null, data)
    }
    callback = null
  }

  dsumDb
    .createValueStream({ reverse: true, valueEncoding: 'json', limit: count })
    .on('error', onErrorOrEnd)
    .pipe(listStream.obj(onErrorOrEnd))
    .on('error', onErrorOrEnd)
}


module.exports.pkgRank      = pkgRank
module.exports.pkgDownloads = pkgDownloads
module.exports.topDownloads = topDownloads