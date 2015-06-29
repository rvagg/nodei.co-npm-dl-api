const moment     = require('moment')
    , listStream = require('list-stream')
    , db         = require('./db')


function pkgRank (pkg, callback) {
  db.packageDb.get(pkg, function (err, data) {
    if (err) {
      if (err.notFound)
        return callback(null, '')
      return callback(err)
    }

    callback(null, JSON.parse(data))
  })
}


function _pkgDownloadDays (pkg, days, callback) {
  var pkgCountDb = db.packageCountDb(pkg)
    , start      = moment().utcOffset(0).subtract(days + 1, 'days').format('YYYY-MM-DD')
    , end        = moment().utcOffset(0).subtract(1, 'days').format('YYYY-MM-DD')

  function onErrorOrEnd (err, data) {
    callback && callback(err, data)
    callback = null
  }

  pkgCountDb
    .createReadStream({ gte: start, lt: end })
    .on('error', onErrorOrEnd)
    .pipe(listStream.obj(onErrorOrEnd))
    .on('error', onErrorOrEnd)
}


function pkgDownloadDays (pkg, days, callback) {
  _pkgDownloadDays(pkg, days, function (err, data) {
    if (err)
      return callback(err)

    var _data = data.map(function (d) {
      return { day: d.key, count: parseInt(d.value, 10) }
    })

    callback(null, _data)
  })
}


function pkgDownloadSum (pkg, days, callback) {
  _pkgDownloadDays(pkg, days, function (err, data) {
    if (err)
      return callback(err)

    var sum = data.reduce(function (p, c) {
      return p + parseInt(c.value, 10)
    }, 0)

    callback(null, sum)
  })
}


function topDownloads (count, callback) {
  var dsumDb = db.dateSumDb(moment().utcOffset(0).subtract(1, 'days').format('YYYY-MM-DD'))

  function onErrorOrEnd (err, data) {
    if (!callback)
      return
    if (err) {
      callback(err)
    } else if (data) {
      data = data.map(function (_d, i) {
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


module.exports.pkgRank         = pkgRank
module.exports.pkgDownloadDays = pkgDownloadDays
module.exports.pkgDownloadSum  = pkgDownloadSum
module.exports.topDownloads    = topDownloads
