const rateLimit      = require('function-rate-limit')
    , downloads      = require('npm-download-counts')
    , listStream     = require('list-stream')
    , moment         = require('moment')
    , log            = require('bole')('process-packages')
    , db             = require('./db')
    , sumPackage     = require('./sum-package')

    , period         = 5 * 60 * 60 * 1000
    , downloadsLimit = rateLimit(100000, period, downloads)


function lastDate (pkg, callback) {
  function collected (err, data) {
    if (err)
      return callback(err)

    return callback(null, data && data[0] && data[0].key)
  }

  try {
    db.packageCountDb(pkg).createReadStream({ reverse: true, limit: 1 }).pipe(listStream.obj(collected))
  } catch (e) {
    log.error('createReadStream error for', pkg)
    log.error(e.stack)
    callback()
  }
}


function save (date, pkg, batch, callback) {
  db.packageCountDb(pkg).batch(batch, function (err) {
    if (err)
      log.error('Error saving count data for %s: %s', pkg, err.message)

    sumPackage(date, pkg, callback)
  })
}


// download data is sparse, so we need to zero-fill it and
// convert it into leveldb batch objects
function downloadDataToBatch (start, end, data) {
  var day   = start
    , batch = []
    , dayS

  var dataMap = data.reduce(function (p, c) {
    p[c.day] = c.count
    return p
  }, {})

  while (day < end) {
    dayS = day.format('YYYY-MM-DD')
    batch.push({ type: 'put', key: dayS, value: dataMap[dayS] || 0 })
    day = day.add('days', 1)
  }

  return batch
}


function processPackage (date, pkg, callback) {
  lastDate(pkg, fetchSince)

  function fetchSince (err, lastDate) {
    if (err) {
      log.error('Last-date fetch error for for %s: %s', pkg, err.message)
      return callback()
    }

    var start = !!lastDate ? moment(lastDate) : moment(date).zone(0).subtract('year', 1)
      , end   = moment(date).zone(0).subtract('day', 1)

    if (start.isSame(end, 'day') || start.isAfter(end, 'day'))
      return sumPackage(date, pkg, callback)

    start = start.subtract('days', 5) // back up a bit just to make sure we have it all

    downloadsLimit(pkg, start.toDate(), end.toDate(), counts)

    function counts (err, data) {
      if (err) {
        log.error('Count fetch error for %s: %s', pkg, err.message)
        if (!/no stats for this package for this range/.test(err.message))
          return sumPackage(date, pkg, callback)
        else
          data = [] // no stats, so zero fill
      }

      if (!Array.isArray(data)) {
        log.error('Unexpected count data for %s: %s', pkg, JSON.stringify(data))
        return sumPackage(date, pkg, callback)
      }

      var batch = downloadDataToBatch(start, end, data)

      save(date, pkg, batch, callback)
    }
  }
}


module.exports = processPackage