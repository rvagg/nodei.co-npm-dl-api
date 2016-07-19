'use strict'

var rateLimit      = require('function-rate-limit')
  , downloads      = require('npm-download-counts')
  , listStream     = require('list-stream')
  , moment         = require('moment')
  , log            = require('bole')('process-packages')
  , db             = require('./db')
  , sumPackage     = require('./sum-package')

  , period         = 10 // 5 * 60 * 60 * 1000
  , downloadsLimit = rateLimit(100000, period, downloads)


function lastDate (pkg, callback) {
  function collected (err, data) {
    var key

    if (err)
      return callback(err)

    key = data && data[0] && data[0].key
    return callback(null, key)
  }

  try {
    db.packageCountDb(pkg)
      .createReadStream({ reverse: true, limit: 1 })
      .pipe(listStream.obj(collected))
  } catch (e) {
    log.error('createReadStream error for ' + pkg)
    log.error(e.stack)
    callback()
  }
}


function save (date, pkg, batch, callback) {
  db.packageCountDb(pkg).batch(batch, function afterBatch (err) {
    if (err)
      log.error('Error saving count data for ' + pkg + ': ' + err.message)
    sumPackage(date, pkg, callback)
  })
}


// download data is sparse, so we need to zero-fill it and
// convert it into leveldb batch objects
function downloadDataToBatch (start, end, data) {
  var day   = start
    , batch = []
    , dayS

  var dataMap = data.reduce(function r (p, c) {
    p[c.day] = c.count
    return p
  }, {})

  while (day < end) {
    dayS = day.format('YYYY-MM-DD')
    batch.push({ type: 'put', key: dayS, value: dataMap[dayS] || 0 })
    day = day.add(1, 'days')
  }

  return batch
}


function processPackage (date, pkg, callback) {
  lastDate(pkg, fetchSince)

  function fetchSince (err, lastDate) {
    var start, end

    if (err) {
      log.error('Last-date fetch error for for ' + pkg + ': ' + err.message)
      return callback()
    }

    start = lastDate && moment(lastDate).subtract(2, 'day')
    end   = moment(date).utcOffset(0)

    if (!lastDate || !start.isValid())
      start = moment(date).utcOffset(0).subtract(1, 'year')

    // console.log(`fetchPackage(${start && start.format('YYYY-MM-DD')}, ${end.format('YYYY-MM-DD')})`)
    if (start.isSame(end, 'day') || start.isAfter(end, 'day'))
      return sumPackage(date, pkg, callback)

    start = start.subtract(5, 'days') // back up a bit just to make sure we have it all

    downloadsLimit(pkg, start.toDate(), end.toDate(), counts)

    function counts (err, data) {
      var batch

      if (err) {
        log.error('Count fetch error for ' + pkg + ': ' + err.message)
        if (!/no stats for this package for this range/.test(err.message))
          return sumPackage(date, pkg, callback)
        else
          data = [] // no stats, so zero fill
      }

      if (!Array.isArray(data)) {
        log.error('Unexpected count data for ' + pkg + ': ' + JSON.stringify(data))
        return sumPackage(date, pkg, callback)
      }

      batch = downloadDataToBatch(start, end, data)
      save(date, pkg, batch, callback)
    }
  }
}


module.exports = processPackage
